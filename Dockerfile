# Used to build an image for the final database
FROM neo4j:4.4-enterprise
COPY ./biocypher_neo4j_volume /data
RUN chown -R 7474:7474 /data
EXPOSE 7474
EXPOSE 7687