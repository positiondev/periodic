{-# LANGUAGE OverloadedStrings #-}
import           Control.Concurrent (forkIO, threadDelay)
import           Control.Monad      (forever)
import qualified Data.Text.IO       as T
import qualified Database.Redis     as R
import           System.Periodic
main = do rconn <- R.connect R.defaultConnectInfo
          scheduler <- create (Name "default") rconn (CheckInterval (Seconds 1)) (LockTimeout (Seconds 1000)) T.putStrLn
          addTask scheduler "print-hello-job" (Every (Seconds 10)) (T.putStrLn "hello")
          addTask scheduler "print-bye-job" (Every (Seconds 1)) (T.putStrLn "bye")
          forkIO (run scheduler)
          forkIO (run scheduler)
          forever (threadDelay 1000000)
