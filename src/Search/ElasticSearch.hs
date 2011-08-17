{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Haskell bindings to the REST servire provided by elasticsearch.
module Search.ElasticSearch
       ( -- * elasticsearch connection
         ElasticSearch(..)
       , localServer

         -- * Documents
       , Document(..)
       , DocumentType(..)

         -- * Indexing
       , indexDocument

         -- * Searching
       , SearchResults(getResults)
       , search
       ) where

import Control.Applicative
import Data.List                  (intercalate)
import Data.Maybe                 (fromJust)

import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Text as T

import Data.Attoparsec.Lazy       (parse, Result(..))
import Data.Aeson                 (FromJSON, ToJSON, encode, json, parseJSON
                                  ,Value(..), (.:))
import Data.Aeson.Types           (parseEither)
import Data.Text                  (Text)
import Network.BufferType         (buf_fromStr, buf_empty, bufferOps, BufferType
                                  ,BufferOp)
import Network.HTTP               (Request(..), RequestMethod(..), simpleHTTP
                                  ,Header(..), HeaderName(..), Response(..))
import Network.URI                (URI(..), parseRelativeReference, relativeTo
                                  ,parseURI)

--------------------------------------------------------------------------------
-- | A type of document, with a phantom type back to the document itself.
newtype DocumentType a = DocumentType { unDocumentType :: String }

--------------------------------------------------------------------------------
-- | A connection an elasticsearch server.
data ElasticSearch = ElasticSearch
    { -- | The URI to @/@ of this instance.
      esEndPoint :: URI

      -- | The user agent to make requests with.
    , esUserAgent :: String
    }

--------------------------------------------------------------------------------
-- | A type class specifying that @doc@ is a document that can be stored within
-- elasticsearch.
class (FromJSON doc, ToJSON doc) => Document doc where
   -- | Gets the key for a given document.
  documentKey  :: doc -> String

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
-- | Run a given search query.
newtype SearchResults d = SearchResults { getResults :: [d] }
                          deriving (Show)

instance (FromJSON d) => FromJSON (SearchResults d) where
  parseJSON (Object v) = SearchResults <$> results
    where results = (v .: "hits") >>= (.: "hits") >>= mapM mapResult
          mapResult r = r .: "_source" >>= parseJSON
  parseJSON _ = empty

search :: forall doc. Document doc
       => ElasticSearch
       -- ^ The elasticsearch server to use.
       -> String
       -- ^ The index to search.
       -> Text
       -- ^ The search query.
       -> IO (SearchResults doc)
       -- ^ A list of matching documents.
search es index query =
    dispatchRequest es GET path Nothing >>= parseSearchResults
  where dt = unDocumentType (documentType :: DocumentType doc)
        path = case combineParts [ index, dt, "_search" ] of
          Nothing -> error "Could not form search query"
          Just uri -> uri { uriQuery = "?q=" ++ T.unpack query }
        parseSearchResults sJson = case parse json sJson of
          (Done rest r) -> case parseEither parseJSON r of
            Right res -> return res
            Left e -> error e

--------------------------------------------------------------------------------
-- Private API
--------------------------------------------------------------------------------

combineParts :: [String] -> Maybe URI
combineParts = parseRelativeReference . intercalate "/"

docType:: forall doc. Document doc => doc -> String
docType _ = unDocumentType ( documentType :: DocumentType doc )

documentIndexPath :: (Document doc) => doc -> String -> URI
documentIndexPath doc index =
  case combineParts [ index, docType doc, documentKey doc ] of
    Nothing -> error "Could not construct document path"
    Just p -> p

dispatchRequest :: ElasticSearch -> RequestMethod -> URI -> Maybe BS.ByteString
                -> IO BS.ByteString
dispatchRequest es method apiCall body =
  case uri' of
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
