{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Search.ElasticSearchSpec where

import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Loops        (unfoldM)
import           Data.Aeson
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.List                  as List
import           Data.Maybe
import qualified Data.Text                  as Text
import           Search.ElasticSearch
import           Test.Hspec
-- TODO get a better bracket pattern, the ES functions throw exceptions.
import           Network.URI

data Tweet = Tweet String String
           deriving (Show,Eq,Ord)

instance Document Tweet where
  documentKey (Tweet a b) = Text.pack $ a ++ b
  documentType = DocumentType "tweet"

instance ToJSON Tweet where
  toJSON (Tweet t u) = object [ "tweet" .= t
                            , "user" .= u ]

instance FromJSON Tweet where
  parseJSON (Object o) = Tweet <$> o .: "tweet"
                               <*> o .: "user"
  parseJSON _ = mzero

fakeBulk :: Document a => ElasticSearch -> String -> t -> [a] -> IO ()
fakeBulk s i _ docs = forM_ docs $ \d -> indexDocument s i d

asyncMap :: (t -> IO a) -> [t] -> IO [a]
asyncMap f [] = return []
asyncMap f (x:xs) = do
  (a,b) <- concurrently (f x) (asyncMap f xs)
  return (a:b)

spec :: Spec
spec = do
  describe "JSON decoders" $ do
    it "should decode a json bulkresult blob" $ do
      let raw ="{ \"index\": { \"_index\": \"twitter\", \"ok\": true, \"_id\": \"hi\", \"_type\": \"tweet\", \"_version\": 1}}"
      decode raw `shouldBe` Just (BulkResult Index True "twitter" "hi" "tweet" 1)

    it "should decode a json bulkresults blob" $ do
      let raw ="{\"took\": 1, \"items\": [ { \"index\": { \"_index\": \"twitter\", \"ok\": true, \"_id\": \"hi\", \"_type\": \"tweet\", \"_version\": 1}}]}"
      decode raw `shouldBe` Just (BulkResults [BulkResult Index True "twitter" "hi" "tweet" 1])

    it "should parse this regression" $ do
      let raw = "{\"took\":5,\"items\":[{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 1Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 2Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 3Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 4Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 5Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 6Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 7Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 8Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 9Ollie\",\"_version\":1,\"ok\":true}},{\"index\":{\"_index\":\"twitter\",\"_type\":\"tweet\",\"_id\":\"hello world 10Ollie\",\"_version\":1,\"ok\":true}}]}"
      (isJust $ (decode raw :: Maybe BulkResults)) `shouldBe` True

  describe "Search.ElasticSearch" $ do
    let twitterIndex = "twitter"
        tweet = Tweet "Hello world!" "Ollie"
        -- localServer = ElasticSearch (fromJust $ parseURI "http://127.0.0.1:9999") "haskell/elasticsearch"

        ignore :: ErrorCall -> IO ()
        ignore s = print s
         -- this should really be a bracket-y thing
        delete = liftIO $ deleteIndex localServer twitterIndex `catch` ignore
        create = liftIO $ createIndex localServer twitterIndex Nothing Nothing

        breathe = liftIO $ waitForYellow localServer >> refresh localServer twitterIndex
        findOllie :: IO (SearchResults Tweet)
        findOllie = search localServer twitterIndex 0 "user:ollie"

    it "can roundtrip a request" $ do
      let tweet = Tweet "Hello world!" "Ollie"

      delete
      create
      breathe
      docs <- liftIO findOllie
      (map result $ getResults docs) `shouldBe` []
      indexDocument localServer twitterIndex tweet
      breathe
      newdocs <- liftIO $ findOllie
      (map result $ getResults newdocs) `shouldBe` [tweet]
      delete

    it "can handle bulk requests" $ do
      let docNum = 20
      let tweets = [ Tweet ("hello world " ++ show x) "Ollie" | x <- [1..docNum]]
      delete
      create
      breathe
      docs <- liftIO $ findOllie
      (map result $ getResults docs) `shouldBe` []
      (failures, successes) <- bulkIndexDocuments localServer twitterIndex (Just 10) tweets
      length successes `shouldBe` docNum
      length failures  `shouldBe` 0
      breathe
      newdocs <- liftIO $ findOllie
      (totalHits newdocs) `shouldBe` (fromIntegral docNum)
      -- check we can multiGet them too
      (MultigetResults fetchedDocs :: MultigetResults Tweet) <- liftIO $ multiGet localServer twitterIndex successes

      (length fetchedDocs) `shouldBe` (fromIntegral $ length successes)

    it "can handle many simultaneous bulk requests" $ do
      pending
      let docNum = 1000
          -- Ollie - this is the variable to tweak. around 10 on my
          -- machine is relatively reliable, higher invariably breaks.
          numThreads = 20
          writerThread :: Int -> IO (SearchResults Tweet, ([Char8.ByteString], [Char8.ByteString]))
          writerThread n = do
            res <- bulkIndexDocuments localServer twitterIndex (Just 47)
            -- res <- fakeBulk localServer twitterIndex (Just 47)
              [ Tweet (show x) (show n) | x <- [1..docNum]]
            breathe
            searched <- search localServer twitterIndex 0 (Text.pack $ "user:" ++ show n)
            return (searched, res)
      delete
      create
      breathe

      results <- liftIO $ asyncMap writerThread [1..numThreads]
      mapM_ (\(_,(f,s)) -> do
                f `shouldBe` []
                length s `shouldBe` docNum)   results
      liftIO $ putStrLn "so ES thinks we're ok"
      map (totalHits . fst) results `shouldBe` replicate numThreads (fromIntegral docNum)
      liftIO $ putStrLn "second"

    it "understands scrolls" $ do
      delete
      create
      breathe
      let input =              [ Tweet (show x) "userkey" | x <- [1..150]]
      _ <- bulkIndexDocuments localServer twitterIndex (Just 47) input
      breathe
      scroller <- scrolledSearch localServer twitterIndex "user:userkey"  (Just 15) ["tweet", "user"]
      -- scrolledRes <- unfoldM scroller
      scrolledRes <- unfoldM scroller
      List.sort (concat scrolledRes) `shouldBe` List.sort input
      print (map length scrolledRes)
      -- we have five shards, so we get 75 results back
      length (head scrolledRes) `shouldBe` 75
  -- it "can handle simultaneous bulk requests" $ do
  --     let docNum = 1000
  --         numThreads = 199
  --         writerThread :: Int -> IO (SearchResults Tweet)
  --         writerThread n = do
  --           bulkIndexDocuments localServer twitterIndex (Just 47)
  --             [ Tweet (show x) (show n) | x <- [1..docNum]]
  --           breathe
  --           search localServer twitterIndex 0 (Text.pack $ "user:" ++ show n)
  --     delete
  --     create
  --     breathe

  --     results <- liftIO $ asyncMap writerThread [1..numThreads]
  --     map totalHits results `shouldBe` replicate numThreads docNum
