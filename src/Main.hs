{-# LANGUAGE OverloadedStrings #-}

import Control.Applicative
import Data.Aeson
import ElasticSearch

newtype Tweet = Tweet String
                deriving (Show)

instance Document Tweet where
  documentKey _ = show (5::Integer)
  documentType = DocumentType "tweet"

instance ToJSON Tweet where
  toJSON (Tweet t) = object [ "tweet" .= t ]

instance FromJSON Tweet where
  parseJSON (Object o) = Tweet <$> o .: "tweet"

main :: IO ()
main = do
  let twitterIndex = "twitter"
      tweet = Tweet "Hello world!"
  indexDocument localServer twitterIndex tweet
  docs <- search localServer twitterIndex "hello" :: IO (SearchResults Tweet)
  print docs
