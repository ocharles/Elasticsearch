{-# LANGUAGE OverloadedStrings #-}

import Control.Applicative
import Data.Aeson
import ElasticSearch

data Tweet = Tweet String String
           deriving (Show)

instance Document Tweet where
  documentKey _ = show (5::Integer)
  documentType = DocumentType "tweet"

instance ToJSON Tweet where
  toJSON (Tweet t u) = object [ "tweet" .= t
                            , "user" .= u ]

instance FromJSON Tweet where
  parseJSON (Object o) = Tweet <$> o .: "tweet"
                               <*> o .: "user"

main :: IO ()
main = do
  let twitterIndex = "twitter"
      tweet = Tweet "Hello world!" "Ollie"
  indexDocument localServer twitterIndex tweet
  docs <- search localServer twitterIndex "user:ollie" :: IO (SearchResults Tweet)
  print docs
