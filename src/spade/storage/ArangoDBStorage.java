package spade.storage;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.*;
import com.arangodb.model.*;

import spade.core.AbstractEdge;
import spade.core.AbstractStorage;
import spade.core.AbstractVertex;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.core.Settings;
import spade.storage.arangodb.Configuration;

public class ArangoDBStorage extends AbstractStorage {
    private ArangoDB arangoDB;
    private ArangoDatabase database;

    private final Configuration configuration = new Configuration();

    @Override
    public boolean initialize(final String arguments) {
        try {
            final String configPath = Settings.getDefaultConfigFilePath(this.getClass());
			this.configuration.load(arguments, configPath);

            String dbName   = configuration.getDbName();
            String host     = configuration.getHost();
            int port        = configuration.getPort();
            String user     = configuration.getDbUser();
            String password = configuration.getDbPassword();

            arangoDB = new ArangoDB.Builder()
                    .host(host, port)
                    .user(user)
                    .password(password)
                    .build();
            database = arangoDB.db(dbName);

            if (!database.exists()) {
                database.create();
            }

            ArangoCollection edgesColl = database.collection("edges");
            if (!edgesColl.exists()) {
                database.createCollection("edges", new CollectionCreateOptions().type(CollectionType.EDGES));
            } else {
                // fetch its metadata
                CollectionEntity edgesInfo = edgesColl.getInfo();
                if (edgesInfo.getType() != CollectionType.EDGES) {
                    // drop and recreate both collections if type mismatch
                    edgesColl.drop();
                    database.collection("vertices").drop();
                    database.createCollection("edges",new CollectionCreateOptions().type(CollectionType.EDGES));
                }
            }

            ArangoCollection verticesColl = database.collection("vertices");
            if (!verticesColl.exists()) {
                database.createCollection("vertices");
            }

            return true;
        } catch (Exception exception) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, null, exception);
            return false;
        }
    }

    @Override
    public boolean storeVertex(AbstractVertex vertex) {
        try {
            BaseDocument document = new BaseDocument();
            document.setKey(vertex.bigHashCode()); // Use vertex hash as the document key
            Map<String, String> annotations = vertex.getCopyOfAnnotations(); // Adjusted method call
            for (Map.Entry<String, String> entry : annotations.entrySet()) {
                document.addAttribute(entry.getKey(), entry.getValue());
            }
            database.collection("vertices").insertDocument(document);
            return true;
        } catch (ArangoDBException ex) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, "ArangoDBException when storing vertex", ex);
            return false;
        } catch (Exception ex) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, "General exception when storing vertex", ex);
            return false;
        }
    }

    @Override
    public boolean storeEdge(AbstractEdge edge) {
        try {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.INFO, "Storing edge with hash: " + edge.bigHashCode() + " from " + edge.getChildVertex().bigHashCode() + " to " + edge.getParentVertex().bigHashCode());
            BaseDocument document = new BaseDocument();
            document.setKey(edge.bigHashCode()); // Use edge hash as the document key
            document.addAttribute("_from", "vertices/" + edge.getChildVertex().bigHashCode());
            document.addAttribute("_to", "vertices/" + edge.getParentVertex().bigHashCode());
            Map<String, String> annotations = edge.getCopyOfAnnotations(); // Adjusted method call
            for (Map.Entry<String, String> entry : annotations.entrySet()) {
                document.addAttribute(entry.getKey(), entry.getValue());
            }
            database.collection("edges").insertDocument(document);
            return true;
        } catch (ArangoDBException ex) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, "ArangoDBException when storing edge", ex);
            return false;
        } catch (Exception ex) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, "General exception when storing edge", ex);
            return false;
        }
    }

    @Override
    public Object executeQuery(String query) { return null; }

    @Override
    public boolean shutdown() {
        try {
            arangoDB.shutdown();
            return true;
        } catch (Exception exception) {
            Logger.getLogger(ArangoDBStorage.class.getName()).log(Level.SEVERE, null, exception);
            return false;
        }
    }
}
