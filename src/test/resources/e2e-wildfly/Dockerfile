# Transit Sims in WildFly: the WAR, unpacked, deployed by the deployment scanner of the WildFly image
FROM quay.io/wildfly/wildfly:41.0.1.Final-jdk25
# server.cli configures the server with the embedded server of jboss-cli.sh, a start of the server; without the
# folders that start writes, which a start creates anew
COPY src/main/wildfly/server.cli /opt/jboss/e2e/
RUN "$JBOSS_HOME/bin/jboss-cli.sh" --file=/opt/jboss/e2e/server.cli \
    && rm -rf "$JBOSS_HOME/standalone/configuration/standalone_xml_history" "$JBOSS_HOME/standalone/data" \
        "$JBOSS_HOME/standalone/log" "$JBOSS_HOME/standalone/tmp"
# standalone.sh with exec. With LAUNCH_JBOSS_IN_BACKGROUND, set in the image, the script runs Java in the background
# and turns its exit code after SIGTERM (143) into 0; in the foreground it runs Java without exec, and the signals of
# the container do not reach Java. So the container runs a copy of the script that starts Java with exec in the
# foreground: Java gets the signals, and its exit code is the one of the container. If sed finds nothing to change in
# a new standalone.sh, the build fails, rather than the test of the exit code after SIGTERM.
RUN sed 's/eval \\"\$JAVA\\" -D/eval exec \\"$JAVA\\" -D/' "$JBOSS_HOME/bin/standalone.sh" \
        > "$JBOSS_HOME/bin/standalone-exec.sh" \
    && grep -q 'eval exec' "$JBOSS_HOME/bin/standalone-exec.sh"
# An unpacked deployment needs the marker .dodeploy; the scanner deploys it in place, without a copy to the content
# repository (standalone/data/content)
COPY --chown=jboss:root target/ROOT /opt/jboss/wildfly/standalone/deployments/ROOT.war
RUN touch /opt/jboss/wildfly/standalone/deployments/ROOT.war.dodeploy
# The data source gets the E2E_DB_* variables as system properties
CMD ["sh", "-c", "LAUNCH_JBOSS_IN_BACKGROUND= exec sh \"$JBOSS_HOME/bin/standalone-exec.sh\" -b 0.0.0.0 -De2e.db.url=\"$E2E_DB_URL\" -De2e.db.user=\"$E2E_DB_USER\" -De2e.db.password=\"$E2E_DB_PASSWORD\""]
