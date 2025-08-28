package eu.neverblink.jelly.cli.graal;

import com.apicatalog.jsonld.JsonLdError;
import com.apicatalog.jsonld.http.HttpClient;
import com.apicatalog.jsonld.http.HttpResponse;
import com.oracle.svm.core.annotate.Delete;
import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

import java.net.URI;
import java.nio.charset.Charset;
import java.security.Provider;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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
 * Use pseudo-random UUIDs instead of secure random ones for seeding the blank node allocator.
 * The secure random number generation pulls in a lot of stuff we don't need.
 * <p>
 * For conversion commands, this is not used at all, because we preserve blank node IDs.
 */
@TargetClass(org.apache.jena.riot.lang.BlankNodeAllocatorHash.class)
final class BlankNodeAllocatorHashSubstitute {
    @Substitute
    UUID freshSeed() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return new UUID(r.nextLong(), r.nextLong());
    }
}

/**
 * Jena uses secure random number generation to create blank node IDs in its Model API.
 * <p>
 * Replaced with pseudo-random UUIDs to avoid including secure random number generation in the native image.
 * This should be good enough for our purposes, because this is only used in init code for vocabularies
 * and not for user data.
 */
@TargetClass(org.apache.jena.graph.BlankNodeId.class)
final class BlankNodeIdSubstitute {
    @Substitute
    public static String createFreshId() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        return new UUID(r.nextLong(), r.nextLong()).toString();
    }
}

/**
 * Disable UTF-32LE support in JSON parsers, which we don't need.
 * This allows us to avoid including all charsets in the native image, which saves quite a bit of space.
 * See: https://github.com/Jelly-RDF/cli/issues/154
 */
@TargetClass(className = "org.glassfish.json.UnicodeDetectingInputStream")
final class UnicodeDetectingInputStreamSubstitute {
    @Delete
    private static Charset UTF_32LE;
}
