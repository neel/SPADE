
package spade.storage.arangodb;

import java.util.Map;

import spade.utility.ArgumentFunctions;
import spade.utility.FileUtility;
import spade.utility.HelperFunctions;

public class Configuration{

	private static final String
		  keyHost = "localhost"
		, keyPort = "8529"
		, keyDatabase = "spade"
		, keyUsername = "root"
		, keyPassword = "";

	private String host;
	private int port;
	private String dbName;
	private String dbUser;
	private String dbPassword;

	public final void load(final String arguments, final String path) throws Exception{
		try{
			final Map<String, String> map = HelperFunctions.parseKeyValuePairsFrom(arguments, new String[]{path});
			host = ArgumentFunctions.mustParseHost(keyHost, map);
			port = ArgumentFunctions.mustParseInteger(keyPort, map);
			dbName = ArgumentFunctions.mustParseNonEmptyString(keyDatabase, map);
			dbUser = ArgumentFunctions.mustParseNonEmptyString(keyUsername, map);
			dbPassword = ArgumentFunctions.mustParseNonNullString(keyPassword, map);
		}catch(Exception e){
			throw new Exception("Failed to read/parse configuration: '" + path + "'", e);
		}
	}

	public String getHost(){
		return host;
	}

	public int getPort(){
		return port;
	}

	public String getDbName(){
		return dbName;
	}

	public String getDbUser(){
		return dbUser;
	}

	public String getDbPassword(){
		return dbPassword;
	}

	@Override
	public String toString(){
		return "Configuration [host=" + host + ", port=" + port + ", dbName=" + dbName + ", dbUser=" + dbUser + ", dbPassword=" + dbPassword + "]";
	}
}
