# jelly-cli

***Warning: This project is in early development and is not yet ready for production use.***

CLI utility for working with Jelly data.

## Quick start

If you are using Linux (x86_64, ARM64), macOS (ARM64), or Windows (x86_64), the recommended way run `jelly-cli` is to use a pre-built binary. Go to the **[releases page](https://github.com/Jelly-RDF/cli/releases)** and download the binary built for your platform.

You can then run it like so:

```shell
$ chmod +x jelly-cli
$ ./jelly-cli --help
```

To convert an RDF file (e.g., Turtle) to Jelly, simply run:

```shell
$ ./jelly-cli rdf to-jelly input.ttl > output.jelly
```

To convert from Jelly to RDF run:

```shell
$ ./jelly-cli rdf from-jelly input.jelly > output.ttl
```

Use the `--help` option to learn more about all the available settings:

```shell
$ ./jelly-cli rdf to-jelly --help
$ ./jelly-cli rdf from-jelly --help
$ ./jelly-cli rdf inspect --help
```

Alternatively, you can use the JAR build, which runs on any platform, as long as you have Java (min. version 17). Go to the **[releases page](https://github.com/Jelly-RDF/cli/releases)** and download the `jelly-cli.jar` file. Then, run it like so:

```shell
java -jar jelly-cli.jar --help
```

We recommend using the binary distribution, because it has way faster startup times and doesn't require you to install Java.

## Developer notes

Run `sbt fixAll` before committing. Your code should be formatted and free of warnings.
The CI checks will not pass if this is not the case.

### Building from source

#### Ahead-of-time compilation (single binary)

- Ensure you have [GraalVM](https://www.graalvm.org/) installed and the `native-image` utility is available in your `PATH`.
- Clone the repository.
- Run `sbt GraalVMNativeImage/packageBin`
- The binary will be available at `./target/graalvm-native-image/jelly-cli`.

#### Über-JAR build (just-in-time)

- Run `sbt assembly`
- The resulting JAR will be in `./target/scala-3.*.*/jelly-cli-assembly-*.jar`
- Run it like: `java -jar <path-to-jar>`
