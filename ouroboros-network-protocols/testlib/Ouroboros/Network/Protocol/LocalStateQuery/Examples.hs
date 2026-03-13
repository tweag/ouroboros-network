{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Ouroboros.Network.Protocol.LocalStateQuery.Examples where

import Ouroboros.Network.Protocol.LocalStateQuery.Client
import Ouroboros.Network.Protocol.LocalStateQuery.Server
import Ouroboros.Network.Protocol.LocalStateQuery.Type (AcquireFailure (..), Target, LeashId)


--
-- Example client
--

-- | An example 'LocalStateQueryClient', which, for each point in the given
-- list, acquires the state for that point, and if that succeeds, returns the
-- result for the corresponding query. When the state could not be acquired,
-- the 'AcquireFailure' is returned instead of the query results.
--
localStateQueryClient
  :: forall block point query result m.
     Applicative m
  => [(Target point, Maybe LeashId, query result)]
  -> LocalStateQueryClient block point query m
                           [(Target point, Maybe LeashId, Either AcquireFailure result)]
localStateQueryClient = LocalStateQueryClient . pure . goIdle []
  where
    goIdle
      :: [(Target point, Maybe LeashId, Either AcquireFailure result)]  -- ^ Accumulator
      -> [(Target point, Maybe LeashId, query result)]                  -- ^ Remainder
      -> ClientStIdle block point query m
                      [(Target point, Maybe LeashId, Either AcquireFailure result)]
    goIdle acc []               = SendMsgDone Nothing $ reverse acc
    goIdle acc ((tgt, leashId, q):ptqs') = SendMsgAcquire tgt leashId $
      goAcquiring acc tgt leashId q ptqs'

    goAcquiring
      :: [(Target point, Maybe LeashId, Either AcquireFailure result)]  -- ^ Accumulator
      -> Target point
      -> Maybe LeashId
      -> query result
      -> [(Target point, Maybe LeashId, query result)]                  -- ^ Remainder
      -> ClientStAcquiring block point query m
                           [(Target point, Maybe LeashId, Either AcquireFailure result)]
    goAcquiring acc pt leashId q ptqss' = ClientStAcquiring {
        recvMsgAcquired = pure $ goQuery q $ \r -> goAcquired ((pt, leashId, Right r):acc) ptqss'
      , recvMsgFailure  = \failure -> pure $ goIdle ((pt, leashId, Left failure):acc) ptqss'
      }

    goAcquired
      :: [(Target point, Maybe LeashId, Either AcquireFailure result)]
      -> [(Target point, Maybe LeashId, query result)]   -- ^ Remainder
      -> ClientStAcquired block point query m
                          [(Target point, Maybe LeashId, Either AcquireFailure result)]
    goAcquired acc [] = SendMsgRelease $ pure $ SendMsgDone Nothing $ reverse acc
    goAcquired acc ((tgt, leashId, qs):ptqss') = SendMsgReAcquire tgt $
      goAcquiring acc tgt leashId qs ptqss'

    goQuery
      :: forall a.
         query result
      -> (result -> ClientStAcquired block point query m a)
         -- ^ Continuation
      -> ClientStAcquired block point query m a
    goQuery q k = SendMsgQuery q $ ClientStQuerying $ \r -> pure $ k r

--
-- Example server
--

-- | An example 'LocalStateQueryServer'. The first function is called to
-- acquire a @state@, after which the second will be used to query the state.
--
localStateQueryServer
  :: forall block point query m state. Applicative m
  => (Target point -> Maybe LeashId -> Either AcquireFailure state)
  -> (forall result. state -> query result -> result)
  -> LocalStateQueryServer block point query m ()
localStateQueryServer acquire answer =
    LocalStateQueryServer $ pure goIdle
  where
    goIdle :: ServerStIdle block point query m ()
    goIdle = ServerStIdle {
        recvMsgAcquire = goAcquiring
      , recvMsgDone = \_mLeashId -> pure ()
      }

    goAcquiring :: Target point -> Maybe LeashId -> m (ServerStAcquiring block point query m ())
    goAcquiring tgt leashId = pure $ case acquire tgt leashId of
      Left failure -> SendMsgFailure failure goIdle
      Right state  -> SendMsgAcquired $ goAcquired leashId state

    goAcquired :: Maybe LeashId -> state -> ServerStAcquired block point query m ()
    goAcquired leashId state = ServerStAcquired {
        recvMsgQuery     = \query ->
          pure $ SendMsgResult (answer state query) $ goAcquired leashId state
      , recvMsgReAcquire = flip goAcquiring leashId
      , recvMsgRelease   = pure goIdle
      }
