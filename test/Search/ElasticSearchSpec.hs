{-# LANGUAGE OverloadedStrings #-}
module Search.ElasticSearchSpec where

import           Control.Applicative
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Aeson
import qualified Data.Text                as Text
import           Search.ElasticSearch
import           Test.Hspec

-- TODO get a better bracket pattern, the ES functions throw exceptions.

data Tweet = Tweet String String
           deriving (Show,Eq)

instance Document Tweet where
  documentKey (Tweet a b) = Text.pack $ a ++ b
  documentType = DocumentType "tweet"

instance ToJSON Tweet where
  toJSON (Tweet t u) = object [ "tweet" .= t
                            , "user" .= u ]

instance FromJSON Tweet where
  parseJSON (Object o) = Tweet <$> o .: "tweet"
                               <*> o .: "user"

fakeBulk s i _ docs = forM_ docs $ \d -> indexDocument s i d

spec :: Spec
spec = describe "Search.ElasticSearch" $ do
  let twitterIndex = "twitter"
      tweet = Tweet "Hello world!" "Ollie"

      ignore :: ErrorCall -> IO ()
      ignore s = print s
         -- this should really be a bracket-y thing
      delete = liftIO $ deleteIndex localServer twitterIndex `catch` ignore
      create = liftIO $ createIndex localServer twitterIndex Nothing Nothing

      -- I wish I didn't have to do this.
      -- breathe = liftIO $ threadDelay 1000000
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
      let docNum = 100000
      let tweets = [ Tweet ("hello world " ++ show x) "Ollie" | x <- [1..docNum]]
      delete
      create
      breathe
      docs <- liftIO $ findOllie
      (map result $ getResults docs) `shouldBe` []
      bulkIndexDocuments localServer twitterIndex (Just 10) tweets
      breathe
      newdocs <- liftIO $ findOllie
      (totalHits newdocs) `shouldBe` docNum

  it "can handle many simultaneous individual requests" $ do
      let docNum = 1000
          numThreads = 199
          writerThread :: Int -> IO (SearchResults Tweet)
          writerThread n = do
            fakeBulk localServer twitterIndex (Just 47)
              [ Tweet (show x) (show n) | x <- [1..docNum]]
            breathe
            search localServer twitterIndex 0 (Text.pack $ "user:" ++ show n)
      delete
      create
      breathe

      threads <- liftIO $ forM [1..numThreads] (async . writerThread)
      results <- liftIO $ mapM wait threads
      map totalHits results `shouldBe` replicate numThreads docNum

  it "can handle simultaneous bulk requests" $ do
      let docNum = 1000
          numThreads = 199
          writerThread :: Int -> IO (SearchResults Tweet)
          writerThread n = do
            bulkIndexDocuments localServer twitterIndex (Just 47)
              [ Tweet (show x) (show n) | x <- [1..docNum]]
            breathe
            search localServer twitterIndex 0 (Text.pack $ "user:" ++ show n)
      delete
      create
      breathe

      threads <- liftIO $ forM [1..numThreads] (async . writerThread)
      results <- liftIO $ mapM wait threads
      map totalHits results `shouldBe` replicate numThreads docNum
