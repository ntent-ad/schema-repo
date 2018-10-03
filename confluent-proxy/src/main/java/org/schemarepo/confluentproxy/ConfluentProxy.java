package org.schemarepo.confluentproxy;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;


import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;

import org.apache.avro.SchemaParseException;
import org.schemarepo.AbstractBackendRepository;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
/**
 * Confluent Schema Repo Proxy
 */
public class ConfluentProxy extends AbstractBackendRepository {

    String schemaRegistryUrl;
    Integer identityMapCapacity;
    CachedSchemaRegistryClient client;
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Inject
    public ConfluentProxy(@Named(Config.CONFLUENT_CONNECTION_STRING) String schemaRegistryUrl, ValidatorFactory validators) {
        super(validators);
        String registryUrl =  System.getenv("CONFLUENT_REGISTRY_URL");
        if (registryUrl != null){
            this.schemaRegistryUrl = registryUrl;
        }
        else{
            this.schemaRegistryUrl = schemaRegistryUrl;
        }
        logger.info("Configuring proxy to: " + this.schemaRegistryUrl);

        this.identityMapCapacity = 1000;
        this.client = new CachedSchemaRegistryClient(this.schemaRegistryUrl, identityMapCapacity);
    }

    @Override
    protected Subject getSubjectInstance(String subjectName) {
        return new ConfluentSubject(subjectName);
    }

    @Override
    protected void registerSubjectInBackend(String subjectName, SubjectConfig config) {
        try {
            if (config.asMap().keySet().isEmpty()){
                client.updateCompatibility(subjectName, "BACKWARD_TRANSITIVE");
            }
            else{
                client.updateCompatibility(subjectName, "NONE"); //TODO: config
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected boolean checkSubjectExistsInBackend(final String subjectName) {
        //return true;

        try {
            client.getLatestSchemaMetadata(subjectName);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }
        try {
            client.getCompatibility(subjectName);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }
        return false;

    }


    @Override
    public void close() throws IOException {
        // nothing to close here. No resources held open.
    }

    SubjectConfig configFromString(String configString) throws IOException {  //TODO: conifg
        Properties props = new Properties();
        SubjectConfig subjectConfig = null;
        if (configString != null) {
            props.load(new StringReader(configString));
            subjectConfig = RepositoryUtil.configFromProperties(props);
        }
        return subjectConfig;
    }

    String configAsString(SubjectConfig subjectConfig) throws IOException {   //TODO: config
        Properties props = new Properties();
        props.putAll(RepositoryUtil.safeConfig(subjectConfig).asMap());
        StringWriter writer = new StringWriter();
        props.store(writer, "SubjectConfig Properties");
        return writer.toString();
    }

    //
    // Inner classes
    //

    class ConfluentSubject extends Subject {

        private SubjectConfig config;       //TODO : config
        private SchemaEntry latest;
        private int subjectId;

        protected ConfluentSubject(String name) {
            super(name);
//            try {
//                if (checkSubjectExistsInBackend(name) == false) {
//                    throw new RuntimeException("Subject named " + name + " does not exist in the database!");
//                }
//
//                SchemaMetadata schemaMetadata = client.getLatestSchemaMetadata(name);
//                subjectId = schemaMetadata.getId();
//                latest = new SchemaEntry(Integer.toString(subjectId), schemaMetadata.getSchema());
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (RestClientException e) {
//                e.printStackTrace();
//            }
        }

        @Override
        public SubjectConfig getConfig() {
            try {
                String compatibility = client.getCompatibility(getName());
                if (compatibility.toLowerCase().equals("backward_transitive")){
                    return SubjectConfig.emptyConfig();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
                return SubjectConfig.emptyConfig();
            }
            return new SubjectConfig.Builder().setValidators(Collections.EMPTY_LIST).build();
        }

        @Override
        public boolean integralKeys() {
            return true;
        }

        @Override
        public SchemaEntry register(String schema) throws SchemaValidationException {


            Schema.Parser parser = new Schema.Parser();
            RepositoryUtil.validateSchemaOrSubject(schema);
            try {
                int regId = client.register(getName(), parser.parse(schema));
                SchemaEntry schemaEntry = new SchemaEntry(String.valueOf(regId), schema);
                latest = schemaEntry;
                return schemaEntry;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            }
            return null;
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

            Schema.Parser parser = new Schema.Parser();
            SchemaEntry schemaEntry = null;
            try {
                int id  = client.getId(getName(), parser.parse(schema));
                schemaEntry = new SchemaEntry(String.valueOf(id), schema);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            }
            catch (SchemaParseException e){
                e.printStackTrace();
            }
            return schemaEntry;
        }

        @Override
        public SchemaEntry lookupById(String id) {

            Schema.Parser parser = new Schema.Parser();

            SchemaEntry schemaEntry = null;
            try {
                String schema = client.getById(Integer.parseInt(id)).toString();
                client.getId(getName(), parser.parse(schema));
                //String schema = client.getBySubjectAndID(getName(), Integer.parseInt(id)).toString();
                //String schema = client.getById(Integer.parseInt(id)).toString();
                schemaEntry = new SchemaEntry(id, schema);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
                //throw e;
            }
            return schemaEntry;
        }

        @Override
        public SchemaEntry latest() {
            try {
                SchemaMetadata schemaMetaData = client.getLatestSchemaMetadata(getName());
                latest = new SchemaEntry(Integer.toString(schemaMetaData.getId()), schemaMetaData.getSchema());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            }
            return latest;
        }

        @Override
        public Iterable<SchemaEntry> allEntries() {
            ArrayList<SchemaEntry> schemaEntryIterable = new ArrayList<SchemaEntry>();
            try {
                ArrayList<Integer> versionArrayList = (ArrayList<Integer>) client.getAllVersions(getName());

                for (Integer entry : versionArrayList) {
                    SchemaMetadata schemaMetadata = client.getSchemaMetadata(getName(), entry);
                    SchemaEntry schemaEntry = new SchemaEntry(Integer.toString(schemaMetadata.getId()), schemaMetadata.getSchema());
                    schemaEntryIterable.add(schemaEntry);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (RestClientException e) {
                e.printStackTrace();
            }
            return schemaEntryIterable;
        }
    }
}
