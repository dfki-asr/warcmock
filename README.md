# warcmock
Yet another HTTP proxy for serving WARC files.

Build and run the web application archive (war) with Maven:

    mvn clean package tomcat7:run

Shutdown the web application with Maven:

    mvn tomcat7:shutdown


You can configure the location of the WARC file in ./src/main/resources/config.properties:

```xml
<configuration>
    <warc>
        <path>/Users/resc01/tmp/archive.warc.gz</path>
    </warc>
    <statistics>
         <uri>http://localhost:8080/statistics</uri>
    </statistics>
</configuration>
```

Configure your HTTP user agent to use as proxy the hostname
and port on which the servlet container runs (e.g., http://localhost:8080/).

    export http_proxy="http://localhost:8080/"
