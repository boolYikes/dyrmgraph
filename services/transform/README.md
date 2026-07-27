# Transform Job Summary

### Structure

example structure for adding tf tasks:

src/main/java/com/dyrmgraph/transform/
├── Tf1.java
├── Tf2.java
├── Tf3.java
└── Tf4.java

All built into one jar

### Execution

KPO launches a container pod with command:

spark-submit \
  --class com.dyrmgraph.transform.Tf1 \
  /opt/transform/transform.jar \
  s3a://my-bucket/input \
  s3a://my-bucket/output

### Dev

Ops:

- Write .java code
- Test and build .jar
- Build dockerized spark app in which the .jar is baked
- container integration test
- scratch & build -> artifact registry (release)
- used by KPO

Notes:

- `mvn test`: run test
- `mvn clean verify` run test & build
- Dockerfile: build it with -f specified from the proj root context
- Configure KPO from the Airflow TF dag accordingly