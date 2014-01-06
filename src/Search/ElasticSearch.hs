{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Haskell bindings to the REST servire provided by elasticsearch.
module Search.ElasticSearch
       ( -- * elasticsearch connection
         ElasticSearch(..)
       , localServer

         -- * Documents
       , Document(..)
       , DocumentType(..)

         -- * Index admin
       , createIndex
       , deleteIndex

         -- * Indexing
       , Index
       , indexDocument
       , bulkIndexDocuments
       , waitForYellow
       , refresh

         -- * Searching
       , SearchResults(getResults, totalHits)
       , SearchResult(score, result)
       , search
       ) where

import           Control.Applicative
import           Data.List                  (intercalate)
import           Data.Maybe                 (fromJust)

import qualified Data.ByteString.Lazy       as BS
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Text                  as T

import           Control.Monad
import           Data.Aeson                 (FromJSON, ToJSON, Value (..),
                                             encode, json, parseJSON, (.:),
                                             (.=))
import           Data.Aeson.Types           (object, parseEither, typeMismatch)
import           Data.Attoparsec.Lazy       (Result (..), parse)
import           Data.List.Split            (chunksOf)
import           Data.Text                  (Text)
import           Network.BufferType         (BufferOp, BufferType, buf_empty,
                                             buf_fromStr, bufferOps)
import           Network.HTTP               (Header (..), HeaderName (..),
                                             Request (..), RequestMethod (..),
                                             Response (..), simpleHTTP)
import           Network.URI                (URI (..), escapeURIString,
                                             isUnescapedInURI, isUnreserved,
                                             parseRelativeReference, parseURI,
                                             relativeTo)

--------------------------------------------------------------------------------
-- | A type of document, with a phantom type back to the document itself.
newtype DocumentType a = DocumentType { unDocumentType :: String }

--------------------------------------------------------------------------------
-- | A connection an elasticsearch server.
data ElasticSearch = ElasticSearch
    { -- | The URI to @/@ of this instance.
      esEndPoint  :: URI

      -- | The user agent to make requests with.
    , esUserAgent :: String
    }

--------------------------------------------------------------------------------
-- | A type class specifying that @doc@ is a document that can be stored within
-- elasticsearch.
class (FromJSON doc, ToJSON doc) => Document doc where
   -- | Gets the key for a given document.
  documentKey  :: doc -> Text

  -- | Gets the type for a given document. The type is used to specify where in
  -- an index this document should be stored.
  documentType :: DocumentType doc

--------------------------------------------------------------------------------
-- | A basic elasticsearch instance. This assumse the server is at
-- @http://127.0.0.1:9200@ and all requests are made with the user agent
-- @haskell/elasticsearch@
localServer :: ElasticSearch
localServer = ElasticSearch { esEndPoint = fromJust localUri
                            , esUserAgent = "haskell/elasticsearch"
                            }
  where localUri = parseURI "http://127.0.0.1:9200/"

--------------------------------------------------------------------------------
-- | The name of an index.
type Index = String

--------------------------------------------------------------------------------
-- | Create an index
createIndex :: ElasticSearch -- ^ The elasticsearch server to use.
            -> String             -- ^ Index name
            -> Maybe Int          -- ^ Shards
            -> Maybe Int          -- ^ Replicas
            -> IO ()
createIndex es name shards replicas =
  void $ dispatchRequest es PUT path settings
  where Just path = parseRelativeReference ("/" ++ name)
        settings  = Just $ encode [a .= b | (a,Just b) <- [("replicas", replicas)
                                                          ,("shards", shards)]]

--------------------------------------------------------------------------------
-- | Delete an index
deleteIndex :: ElasticSearch -- ^ The elasticsearch server to use.
            -> String             -- ^ Index name
            -> IO ()
deleteIndex es name  =
  void $ dispatchRequest es DELETE path settings
  where Just path = parseRelativeReference ("/" ++ name)
        settings  = Just $ encode ([] :: [Int])


--------------------------------------------------------------------------------
-- | Index a given document, under a given index.
indexDocument :: (Document a)
              => ElasticSearch  -- ^ The elasticsearch server to use.
              -> String         -- ^ The index to index this document under.
              -> a              -- ^ The document to index.
              -> IO ()
indexDocument es index document =
    dispatchRequest es PUT path docJson >> return ()
  where path = documentIndexPath document index
        docJson = Just $ encode document

--------------------------------------------------------------------------------
-- | Index a given document, under a given index.
bulkIndexDocuments :: (Document a)
                   => ElasticSearch     -- ^ The elasticsearch server to use.
                   -> String            -- ^ The index to index this document under.
                   -> Maybe Int         -- ^ Optional chunk size
                   -> [a]               -- ^ The documents to index.
                   -> IO ()
                   -- this may need to change to guarantee a
                   -- bounded-memory usage pattern.
bulkIndexDocuments es index mchunk documents  =
    mapM_ (dispatchRequest es PUT bulkEndpoint . Just . aggregateRep index) $
      chunksOf chunkSize documents
  where -- path = documentIndexPath document index
        chunkSize = maybe 1000 id mchunk
        Just bulkEndpoint = parseRelativeReference "/_bulk"

aggregateRep :: forall doc. Document doc => String -> [doc] -> Char8.ByteString
aggregateRep index docs = (Char8.intercalate "\n" $ map inlineRep docs) `Char8.append` "\n"
  where
    inlineRep :: Document a => a -> Char8.ByteString
    inlineRep doc = Char8.intercalate "\n"
                    [ encode (object ["index" .= object [
                                         "_index".= index,
                                         "_type" .= t,
                                         "_id"   .= documentKey doc ]])
                    , encode doc]
      where t :: String
            t = unDocumentType (documentType :: DocumentType doc)

--------------------------------------------------------------------------------
-- | Run a given search query.
data SearchResults d = SearchResults { getResults :: [SearchResult d]
                                     , totalHits  :: Integer
                                     }

data SearchResult d = SearchResult { score  :: Double
                                   , result :: d
                                   }

instance (FromJSON d) => FromJSON (SearchResults d) where
  parseJSON (Object v) = SearchResults <$> results <*> hits
    where results = (v .: "hits") >>= (.: "hits") >>= mapM parseJSON
          hits = (v .: "hits") >>= (.: "total")
  parseJSON v = typeMismatch "SearchResults" v

instance (FromJSON d) => FromJSON (SearchResult d) where
  parseJSON (Object o) = SearchResult <$> o .: "_score"
                                      <*> (o .: "_source" >>= parseJSON)
  parseJSON v = typeMismatch "SearchResult" v

search :: forall doc. Document doc
       => ElasticSearch
       -- ^ The elasticsearch server to use.
       -> String
       -- ^ The index to search.
       -> Integer
       -- ^ The offset into the results.
       -> Text
       -- ^ The search query.
       -> IO (SearchResults doc)
       -- ^ A list of matching documents.
search es index offset query =
    dispatchRequest es GET path Nothing >>= parseSearchResults
  where dt = unDocumentType (documentType :: DocumentType doc)
        path = case combineParts [ index, dt, "_search" ] of
          Nothing -> error "Could not form search query"
          Just uri ->
            uri { uriQuery = "?" ++ queryString }
        parseSearchResults sJson = case parse json sJson of
          (Done _ r) -> case parseEither parseJSON r of
            Right res -> return res
            Left e -> error e
          (Fail a b c) -> error $ show (a,b,c)
        queryString = intercalate "&" $ (\(k, v) -> k ++ "=" ++ v) `map` queryParts
        queryParts = [ ("q", escapeURIString isUnescapedInURI (T.unpack query))
                     , ("from", show offset)
                     ]

refresh es index = do
  void $ dispatchRequest es GET path Nothing
    where Just path = parseRelativeReference (index ++ "/_refresh")

waitForYellow :: ElasticSearch -> IO ()
waitForYellow es = void $ dispatchRequest es GET path Nothing
    where Just path = parseRelativeReference ("/_cluster/health?wait_for_status=yellow")

--------------------------------------------------------------------------------
-- Private API
--------------------------------------------------------------------------------

combineParts :: [String] -> Maybe URI
combineParts = parseRelativeReference . intercalate "/"

docType:: forall doc. Document doc => doc -> String
docType _ = unDocumentType ( documentType :: DocumentType doc )

documentIndexPath :: (Document doc) => doc -> String -> URI
documentIndexPath doc index =
  case combineParts [ index, docType doc, key doc ] of
    Nothing -> error ("Could not construct document path: " ++ show [index, docType doc, key doc])
    Just p -> p
  where key = escapeURIString isUnreserved . T.unpack . documentKey

dispatchRequest :: ElasticSearch -> RequestMethod -> URI -> Maybe BS.ByteString
                -> IO BS.ByteString
dispatchRequest es method apiCall body =
  case Just uri' of
    Nothing -> error "Could not formulate correct API call URI"
    Just uri -> do
      resp' <- simpleHTTP (req uri)
      case resp' of
        Left e -> error ("Failed to dispatch request: " ++ show e)
        Right resp -> case rspCode resp of
          (2, _, _) -> return (rspBody resp)
          (3, _, _) -> error "Found redirect, dunno what to do"
          (4, _, _) -> error ("Client error: " ++ show (rspBody resp))
          (5, _, _) -> error ("Server error: " ++ show (rspBody resp))
          code      -> error $ "Unknown error occured" ++
                               show (formatResponseCode code)
  where req uri = Request { rqMethod = method
                          , rqURI =  uri
                          , rqHeaders = [ Header HdrUserAgent (esUserAgent es)
                                        , Header HdrContentLength bodyLength ]
                          , rqBody = putBody body
                          }
        uri' = apiCall `relativeTo` esEndPoint es
        putBody (Just body') = buf_fromStr bops $ Char8.unpack body'
        putBody Nothing      = buf_empty bops
        bodyLength           = maybe "0" (show . BS.length) body

bops :: BufferOp BS.ByteString
bops = bufferOps

formatResponseCode :: (Int, Int, Int) -> Int
formatResponseCode (x, y, z) = x*100 + y*10 + z
