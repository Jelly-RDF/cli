package eu.neverblink.jelly.cli.graal;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.http.HttpClient;
import com.apicatalog.jsonld.http.HttpResponse;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import java.net.URI;
import java.security.Provider;
import java.util.List;

// Substitutions of classes and methods for GraalVM native image builds.
// These try to remove things from the static analysis (and, in effect, from the final binary)
// that we don't need in jelly-cli, and that would bloat the binary size.

/**
 * Class used by JSON-LD to make HTTP requests for contexts and stuff.
 * That's a security risk by itself... but also, we don't have to support *every single*
 * JSON-LD feature in jelly-cli.
 */
@Substitute
@TargetClass(com.apicatalog.jsonld.http.DefaultHttpClient.class)
final class DefaultHttpClientSubstitute {
    @Substitute
    public static HttpClient defaultInstance() {
        return new HttpClient() {
            @Override
            public HttpResponse send(URI targetUri, String requestProfile) throws JsonLdError {
                throw new UnsupportedOperationException(
                    "jelly-cli binaries do not support making HTTP requests when processing JSON-LD documents. " +
                    "If you need this functionality, please use the JAR version of jelly-cli."
                );
            }
        };
    }
}

/**
 * Mostly needed for SPARQL over HTTP, which we don't do in jelly-cli.
 */
@Substitute
@TargetClass(org.apache.jena.http.HttpLib.class)
final class HttpLibSubstitute { }

/**
 * Also mostly needed for SPARQL over HTTP, which we don't do in jelly-cli.
 */
@Substitute
@TargetClass(org.apache.jena.http.HttpEnv.class)
final class HttpEnvSubstitute { }

/**
 * UUID generation is used by Jena for blank node IDs, but we can fall back to the default implementation.
 */
@Substitute
@TargetClass(className = "sun.security.jca.ProviderList")
final class ProviderListSubstitute { 
    @Substitute
    public List<Provider> providers() {
        return List.of();
    }
}
