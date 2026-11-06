# Transit Sims in Jetty: the WAR, unpacked, in the Jetty image. A packed WAR is unpacked into a temporary folder at
# every start.
FROM jetty:12.1.14-jdk25
# The modules of Jakarta EE 11, in start.d
RUN java -jar "$JETTY_HOME/start.jar" --add-modules=ee11-deploy,ee11-cdi-decorate
# The format of java.util.logging, and the FINE messages of Doer
COPY src/main/jetty/logging.properties /opt/e2e/
# The command line of the server: without jetty.start the entrypoint of the image computes it with
# start.jar --dry-run, in a JVM of its own, at every start. A system property for the server is an argument of
# start.jar, not of the java that runs it; when jetty.start exists, the arguments of the command are not read.
RUN /generate-jetty-start.sh java -jar "$JETTY_HOME/start.jar" \
        -Djava.util.logging.config.file=/opt/e2e/logging.properties
# The folder ROOT is the context /
COPY --chown=jetty:jetty target/ROOT /var/lib/jetty/webapps/ROOT
# The entrypoint of the image runs jetty.start with exec. Jetty and java.util.logging write to stderr, and the tests
# read stdout, so stderr goes to stdout.
CMD ["sh", "-c", "exec /docker-entrypoint.sh java -jar \"$JETTY_HOME/start.jar\" 2>&1"]
