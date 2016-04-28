package org.schemarepo.jdbc;

import org.schemarepo.AbstractBackendRepository;
import org.schemarepo.InMemorySchemaEntryCache;
import org.schemarepo.RepositoryUtil;
import org.schemarepo.SchemaEntry;
import org.schemarepo.SchemaValidationException;
import org.schemarepo.Subject;
import org.schemarepo.SubjectConfig;
import org.schemarepo.ValidatorFactory;
import org.schemarepo.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

/**
 * JDBC based backend repository.
 */
public class JdbcRepository extends AbstractBackendRepository {
    String jdbc;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Inject
    public JdbcRepository(@Named(Config.JDBC_CONNECTION_STRING) String jdbcConnectionString, ValidatorFactory validators) {
        super(validators);

        this.jdbc = jdbcConnectionString;

        // eagerly load up subjects
        loadSubjects();
    }

    @Override
    protected Subject getSubjectInstance(String subjectName) {
        return new DbSubject(subjectName);
    }

    @Override
    protected void registerSubjectInBackend(String subjectName, SubjectConfig config) {
        Connection conn;
        try {
            int topicId;
            conn = connect(jdbc);

            try {
                // check if subject exists
                topicId = checkExists(subjectName, conn);

                if (topicId >= 0)
                    return;

                // topic does not exist in db, create it
                PreparedStatement ste = conn.prepareStatement(
                        "insert into Topic(Topic,Configuration) values(?,?);");
                ste.setString(1, subjectName);
                ste.setString(2, configAsString(config));
                ste.execute();

            } finally {
                conn.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected boolean checkSubjectExistsInBackend(final String subjectName) {
        Connection conn;
        try {
            int topicId;
            conn = connect(jdbc);

            try {
                // check if subject exists
                topicId = checkExists(subjectName, conn);
                return topicId >= 0;
            } finally {
                conn.close();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private static Connection connect(String jdbc) throws ClassNotFoundException, SQLException {
        return DriverManager.getConnection(jdbc);
    }

    private void loadSubjects() {
        try {
            Connection conn = connect(jdbc);
            try {
                PreparedStatement ste = conn.prepareStatement(
                        "select Topic, Id, Configuration from Topic");
                ResultSet res = ste.executeQuery();
                while (res.next()) {
                    String subjectName = res.getString(1);
                    //int id = res.getInt(2);
                    SubjectConfig subjectConfig = configFromString(res.getString(3));
                    Subject subj = new DbSubject(subjectName, subjectConfig, conn);
                    cacheSubject(subj);
                }
            } finally {
                conn.close();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int checkExists(String subjectName, Connection conn) throws SQLException {
        // topic is not cached
        PreparedStatement ste = conn.prepareStatement("select Id from Topic where Topic=? FOR UPDATE;");
        ste.setString(1, subjectName);
        ResultSet res = ste.executeQuery();
        if(res.next()) {
            int id = res.getInt(1);
            return id;
        }

        return -1;
    }

    @Override
    public void close() throws IOException {
        // nothing to close here. No resources held open.
    }

    SubjectConfig configFromString(String configString) throws IOException {
        Properties props = new Properties();
        SubjectConfig subjectConfig = null;
        if (configString != null) {
            props.load(new StringReader(configString));
            subjectConfig = RepositoryUtil.configFromProperties(props);
        }
        return subjectConfig;
    }

    String configAsString(SubjectConfig subjectConfig) throws IOException {
        Properties props = new Properties();
        props.putAll(RepositoryUtil.safeConfig(subjectConfig).asMap());
        StringWriter writer = new StringWriter();
        props.store(writer,"SubjectConfig Properties");
        return writer.toString();
    }

    //
    // Inner classes
    //

    class DbSubject extends Subject {
        private SubjectConfig config;
        private final InMemorySchemaEntryCache schemas = new InMemorySchemaEntryCache();
        private SchemaEntry latest = null;
        private final int subjectId;

        protected DbSubject(String name) {
            super(name);
            Connection conn;
            try {
                conn = connect(jdbc);
                try {
                    subjectId = checkExists(name, conn);
                    if (subjectId < 0) {
                        throw new RuntimeException("Subject named " + name + " does not exist in the database!");
                    }
                    // topic is already registered, just load config and schemas
                    loadConfig(conn);
                    loadSchemas(conn);
                } finally {
                    conn.close();
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /***
         * Private constructor for eager loading of subjects during creation of JdbcRepository.
         * @param name The Subject Name
         * @param config The Subject Config
         * @param conn The open Jdbc Connection to use for lookups and loading of schemas
         * @throws SQLException
         */
        private DbSubject(String name, SubjectConfig config, Connection conn) throws SQLException {
            super(name);
            subjectId = checkExists(name, conn);
            if (subjectId < 0) {
                throw new RuntimeException("Subject named " + name + " does not exist in the database!");
            }
            this.config = RepositoryUtil.safeConfig(config);
            loadSchemas(conn);
        }

        private void loadConfig(Connection conn) throws SQLException, IOException {
            PreparedStatement ste = conn.prepareStatement(
                    "select Configuration from Topic where Id=?");
            ste.setInt(1, subjectId);
            ResultSet res = ste.executeQuery();
            if (!res.next()) {
                throw new RuntimeException("Subject does not exist in database! " + this.getName() + "[" + subjectId + "]");
            }
            SubjectConfig subjectConfig = configFromString(res.getString(1));
            config = RepositoryUtil.safeConfig(subjectConfig);
        }

        @Override
        public SubjectConfig getConfig() {
            return config;
        }

        @Override
        public boolean integralKeys() {
            return true;
        }

        @Override
        public SchemaEntry register(String schema) throws SchemaValidationException {
            RepositoryUtil.validateSchemaOrSubject(schema);
            SchemaEntry entry = loadOrCreateSchema(schema);
            schemas.add(entry);
            latest = entry;
            return entry;
        }

        @Override
        public SchemaEntry registerIfLatest(String schema, SchemaEntry latest) throws SchemaValidationException {
            if (latest == this.latest || (latest != null && latest.equals(this.latest)))
                return register(schema);
            else
                return null;
        }

        @Override
        public SchemaEntry lookupBySchema(String schema) {
            SchemaEntry res = schemas.lookupBySchema(schema);
            if(res != null)
                return res;

            try {
                Connection conn = connect(jdbc);
                loadSchemas(conn);
            } catch(ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch(SQLException e) {
                throw new RuntimeException(e);
            }

            return schemas.lookupBySchema(schema);
        }

        @Override
        public SchemaEntry lookupById(String id) {
            SchemaEntry res = schemas.lookupById(id);
            if(res != null)
                return res;

            try {
                Connection conn = connect(jdbc);
                loadSchemas(conn);
            } catch(ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch(SQLException e) {
                throw new RuntimeException(e);
            }

            return schemas.lookupById(id);
        }

        @Override
        public SchemaEntry latest() {
            return latest;
        }

        @Override
        public Iterable<SchemaEntry> allEntries() {
            return schemas.values();
        }

        //
        // Helper functions
        //

        SchemaEntry loadOrCreateSchema(String schema) {
            int schemaId;
            Connection conn;
            try {
                conn = connect(jdbc);

                try {
                    // check if schema exists
                    String hash = makeHash(schema);
                    PreparedStatement ste = conn.prepareStatement(
                                    "select Id, Hash from `Schema` where Hash=? FOR UPDATE");
                    ste.setString(1, hash);
                    ResultSet res = ste.executeQuery();
                    if (res.next()) {
                        // schema is already registered, just cache it
                        schemaId = res.getInt(1);
                        if (!hash.toLowerCase().equals(res.getString(2).toLowerCase()))
                            throw new RuntimeException("Corrupt schema: hash does not match to the one already exists");
                    } else {
                        // schema does not exist in db, create it
                        PreparedStatement ste2 = conn.prepareStatement(
                                "insert into `Schema`(`Schema`, Hash) values(?,?);\n" +
                                        "select Id from `Schema` where Hash=?");
                        ste2.setString(1, schema);
                        ste2.setString(2, hash);
                        ste2.setString(3, hash);
                        ResultSet res2 = ste2.executeQuery();
                        res2.next();
                        schemaId = res2.getInt(1);
                    }

                    //
                    // Register this schema to this topic
                    //
                    ste = conn.prepareStatement(
                                    "insert ignore into TopicSchemaMap(TopicId, SchemaId) values(?, ?)");
                    ste.setInt(1, subjectId);
                    ste.setInt(2, schemaId);
                    ste.setInt(3, subjectId);
                    ste.setInt(4, schemaId);
                    ste.execute();
                } finally {
                    if (conn != null)
                        conn.close();
                }

                if(schemaId != -1)
                    return new SchemaEntry(String.valueOf(schemaId), schema);
                else
                    throw new RuntimeException("Schema not found");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        private void loadSchemas(Connection conn) throws SQLException {

            // load schemas into subjects
            PreparedStatement ste = conn.prepareStatement("select s.`Schema`, s.Id\n" +
                    "from TopicSchemaMap m\n" +
                    "join `Schema` s on s.Id=m.SchemaId\n" +
                    "where m.TopicId=?\n" +
                    "order by s.Id");
            ste.setInt(1, subjectId);
            ResultSet res = ste.executeQuery();
            while(res.next()) {
                String schema = res.getString(1);
                int schemaId = res.getInt(2);
                SchemaEntry se = new SchemaEntry(String.valueOf(schemaId), schema);
                this.schemas.add(se);
                latest = se;
            }
        }

        private String makeHash(String schema) throws NoSuchAlgorithmException {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buff = schema.getBytes(Charset.forName("US-ASCII"));
            byte[] hash = md.digest(buff);

            return UUID.nameUUIDFromBytes(hash).toString();
        }
    }
}
