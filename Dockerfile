FROM k8-nexus.cb.ntent.com:7443/ntent/centos7-jdk8 
#ARG JAR_FILE
#ARG CLASS_NAME
ADD schema-repo-confluent-proxy-bundle-0.1.3-SNAPSHOT-withdeps.jar app.jar
ADD confluent-proxy-config.properties properties
EXPOSE 2876
ENV NTENT_ENV=dev
ENV CONFLUENT_REGISTRY_URL=http://192.168.99.100:8081
CMD java -jar app.jar properties

#CMD ["java", "-Xms350m", "-Xmx350m", "-Dntent.env=${NTENT_ENV}", "-jar", "./app.jar" , "confluent-proxy-config.properties"]
#CMD ["sh", "-c", "exec java -Xms700m -Xmx700m -Dcom.sun.management.jmxremote.port=1091 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dntent.env=$NTENT_ENV -Dlog.app.name=$LOG_APP_NAME -cp ./app.jar $CLASS_NAME"]
