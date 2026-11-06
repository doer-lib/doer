# Transit Sims in Open Liberty: server.xml, the Postgres driver and the WAR, unpacked, in /config of the Open Liberty
# image. A data source of server.xml cannot use the driver of the WAR, so the driver is also a file next to it.
FROM icr.io/appcafe/open-liberty:26.0.0.10-full-java25-openj9-ubi-minimal
COPY --chown=1001:0 src/main/liberty/server.xml target/liberty/postgresql.jar /config/
COPY --chown=1001:0 target/ROOT /config/apps/ROOT.war
# configure.sh of the image, as its documentation asks after the configuration: it starts the server three times to
# fill the shared class cache of OpenJ9 and keeps the workarea (the resolved features, the caches of the OSGi bundles
# and of the annotations of the application). The application fails to start there, there is no database (Flyway);
# no request to warm it up.
RUN WARM_ENDPOINT=false WARM_OPENAPI_ENDPOINT=false configure.sh
# The command of the image: server run defaultServer
