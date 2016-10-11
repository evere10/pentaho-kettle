package org.pentaho.di.trans.steps.pgbulkloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.util.StreamLogger;

public class PGErrorStreamLogger extends StreamLogger {
  private static final int MAX_LOG_LINES = 10;

  private InputStream is;

  private String type;

  private LogChannelInterface log;

  public PGErrorStreamLogger( LogChannelInterface log, InputStream is, String type ) {
    super( log, is, type, false );
    this.log = log;
    this.is = is;
    this.type = type;
  }

  @Override
  public void run() {
    try (BufferedReader br = new BufferedReader( new InputStreamReader( is ) ) ) {
      int lineCounter = 0;
      String line = null;
      while ( ( line = br.readLine() ) != null ) {
        if ( ++lineCounter <= MAX_LOG_LINES ) {
          log.logError( type + " " + line );
        }
      }
      if ( lineCounter > MAX_LOG_LINES ) {
        log.logError( "psql output cut to " + MAX_LOG_LINES + " lines" );
      }
    } catch ( IOException ioe ) {
      log.logError( type + " " + Const.getStackTracker( ioe ) );
    }
  }
}
