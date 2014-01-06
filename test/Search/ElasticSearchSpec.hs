{-# LANGUAGE OverloadedStrings #-}
module Search.ElasticSearchSpec where

import           Control.Applicative
import           Control.Concurrent
import           Control.Exception
import           Control.Monad.IO.Class
import           Data.Aeson
import qualified Data.Text              as Text
import           Search.ElasticSearch
import           Test.Hspec


-- TODO work out why we need to sleep for a second
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
