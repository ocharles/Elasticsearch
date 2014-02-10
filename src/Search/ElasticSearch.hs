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
       , BulkResults(..)
       , BulkResult(..)
       , Operation (..)


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
       , scrolledSearch

         -- * multiGet api
       , MultigetResults(..)
       , multiGet
       ) where

import           Control.Applicative
import           Data.List                  (intercalate)
import           Data.Maybe                 (catMaybes, fromJust, fromMaybe,
                                             isNothing, maybeToList)

import qualified Data.ByteString.Lazy       as BS
import qualified Data.ByteString.Lazy.Char8 as Char8
import qualified Data.Text                  as T

import           Control.Monad
import           Data.Aeson                 (FromJSON, ToJSON, Value (..),
                                             decode, encode, json, parseJSON,
                                             (.:), (.=))
import           Data.Aeson.Types           (object, parseEither, typeMismatch)
import           Data.Attoparsec.Lazy       (Result (..), parse)
import           Data.IORef
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
-- | The result type from a bulk indexing operation

data Operation = Index
               deriving (Show,Eq)
data BulkResults = BulkResults [BulkResult]
                 deriving (Show,Eq)
data BulkResult = BulkResult { br_op      :: Operation
                             , br_ok      :: Bool
                             , br_index   :: Char8.ByteString
                             , br_id      :: Char8.ByteString
                             , br_type    :: Char8.ByteString
                             , br_version :: Int
                             }
                deriving (Show,Eq)

instance FromJSON BulkResults where
  parseJSON (Object o) = do
    res <- mapM parseJSON =<< (o .: "items")
    return $ BulkResults res
  parseJSON _ = mzero

-- TODO not just "index"
instance FromJSON BulkResult where
  parseJSON (Object o) = do
    body <- o .: "index"
    BulkResult
      <$> return Index
      <*> body .: "ok"
      <*> body .: "_index"
      <*> body .: "_id"
      <*> body .: "_type"
      <*> body .: "_version"
  parseJSON _ = mzero

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
                   -> IO ([Char8.ByteString], [Char8.ByteString])
                                        -- ^ successful & unsuccessful edits
bulkIndexDocuments es index mchunk documents  = do
    results <- mapM (dispatchRequest es PUT bulkEndpoint . Just . aggregateRep index) $
               chunksOf chunkSize documents
    let parsed = map decode results :: [Maybe BulkResults]
        goodParsed = (concatMap (\(BulkResults b) -> b) $ catMaybes parsed) :: [BulkResult]
        failures = map br_id $ filter (not . br_ok) goodParsed
        successes = map br_id $ filter br_ok goodParsed

    forM_ (filter (isNothing . fst) $ zip parsed results) $ \(_,x) -> do
      putStrLn "Bad parse"
      print x

    return (failures, successes)
  where
    defaultChunkSize = 1000
    chunkSize = fromMaybe defaultChunkSize mchunk
    Just bulkEndpoint = parseRelativeReference "/_bulk"

aggregateRep :: forall doc. Document doc => String -> [doc] -> Char8.ByteString
aggregateRep index docs = (Char8.intercalate "\n" $ map inlineRep docs) `Char8.append` "\n"
  where
    inlineRep :: Document a => a -> Char8.ByteString
    inlineRep doc = Char8.intercalate "\n"
                    [ encode (object ["index" .= object [
                                         "_index".= index,
                                         "_type" .= t,
                                         "_refresh" .= True,
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
data MultigetResults d = MultigetResults { mgGetResults :: [d] }
type Field = Text


instance (FromJSON d) => FromJSON (MultigetResults d) where
  parseJSON (Object v) = MultigetResults <$> results
    where results = do
            d <- (v .: "docs")
            mapM (\x -> x .: "_source" >>= parseJSON) d

  parseJSON v = typeMismatch "MultigetResults" v


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

        queryString = intercalate "&" $ (\(k, v) -> k ++ "=" ++ v) `map` queryParts
        queryParts = [ ("q", escapeURIString isUnescapedInURI (T.unpack query))
                     , ("from", show offset)
                     ]

multiGet :: forall doc . Document doc =>
            ElasticSearch -> String -> [Char8.ByteString] -> IO (MultigetResults doc)
multiGet es index ids = dispatchRequest es POST path body >>= parseSearchResults
  where
    Just path = parseRelativeReference $ intercalate "/" [index, dt, "_mget"]
    dt = unDocumentType (documentType :: DocumentType doc)
    body = Just $ encode $ object ["ids" .= ids]

data ScrollResponse = ScrollResponse { unScroll     :: Char8.ByteString,
                                       responseHits :: Int
                                     }

instance FromJSON ScrollResponse where
  parseJSON (Object v) = do
    h <- v .: "hits"
    t <- h .: "total"
    ScrollResponse <$> v .: "_scroll_id" <*> return t

  parseJSON _ = mzero

data ScanResponse a = ScanResponse { unScan :: a }

instance (FromJSON a) => FromJSON (ScanResponse a) where
  parseJSON (Object v) = ScanResponse <$> (v .: "_source" >>= parseJSON)
  parseJSON v = typeMismatch "ScanResponse" v



scrolledSearch :: forall doc . Document doc => ElasticSearch -> String  -> Text -> Maybe [Field] -> IO (IO (Maybe [doc]))
scrolledSearch es index query mfields = do
  print ("body", body)
  resp <- dispatchRequest es POST path body
  putStrLn $ Char8.unpack resp
  let Just (ScrollResponse scrollId totHits) = decode resp
  print (scrollPath,scrollId)
  hitsLeft <- newIORef totHits -- fix
  return $ do
    left <- readIORef hitsLeft
    case left of
      0 -> return Nothing
      _ -> do
        putStrLn ("Running scroll query, " ++ show left ++" left.")
        resp <- dispatchRequest es POST scrollPath (Just scrollId)
        print ("scrolledresp", resp)
        (results :: SearchResults doc)  <- parseSearchResults resp
        let currHits = totalHits results
        print ("scrolled", totalHits results)
        let theHits = map result . getResults $ results
        atomicModifyIORef hitsLeft (\x -> (x-(length theHits), ()))
        return $ Just theHits

  where path = case combineParts [ index, dt, "_search" ] of
          Nothing -> error "Could not form search query"
          Just uri -> uri { uriQuery = "?" ++ queryString }

        queryString = intercalate "&" $ (\(k, v) -> k ++ "=" ++ v) `map` queryParts
        queryParts = [ ("search_type", "scan")
                     , ("scroll",      "5m")
                     , ("size",        "20")]

        -- this is bletcherous. should encode all possible queries as
        -- a data type
        maybefields = map ("fields" .=) $ maybeToList mfields

        body = Just $ encode $ object (maybefields ++ [
                                          "query"  .= object [
                                             "query_string" .= object [
                                                "query" .= query ]]])

        scrollPath = case combineParts [ "_search", "scroll" ] of
          Nothing -> error "could not form scroll query"
          Just uri -> uri { uriQuery = "?scroll=5m" }
        dt = unDocumentType (documentType :: DocumentType doc)

refresh :: ElasticSearch -> String -> IO ()
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
      print ("trying", uri,body)
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

parseSearchResults :: (Monad m, FromJSON a) => Char8.ByteString -> m a
parseSearchResults sJson = case parse json sJson of
          (Done _ r) -> case parseEither parseJSON r of
            Right res -> return res
            Left e -> error e
          (Fail a b c) -> error $ show (a,b,c)
