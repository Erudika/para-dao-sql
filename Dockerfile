FROM openjdk:11-jre-slim

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-dao-sql" \
	PARA_PLUGIN_VER="1.38.2"

ADD https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
