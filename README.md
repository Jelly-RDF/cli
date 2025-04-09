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

## Usage

### Convert RDF to Jelly

To convert an RDF file (e.g., Turtle) to Jelly, simply run:

```shell
$ ./jelly-cli rdf to-jelly input.ttl > output.jelly
```

### Convert Jelly to RDF

To convert from Jelly to RDF run:

```shell
$ ./jelly-cli rdf from-jelly input.jelly > output.nq
```

By default, `jelly-cli` will translate files to NQuads. 
But you can also specify the output format with `--out-format`, for example:

```shell
$ ./jelly-cli rdf from-jelly input.jelly --out-format=ttl > output.ttl
```

You can specify most well-known formats supported by Apache Jena, but also a custom Jelly-Text format. 
Jelly-Text is a human-readable translation of Jelly binary. It's not meant for machine consumption. It is useful for debugging and inspecting Jelly files.

### Inspect Jelly files

To inspect a Jelly file and get basic information describing its contents, such as stream options or number of triples in the file, run

```shell
$ ./jelly-cli rdf inspect input.jelly
```

You can also compute the statistics separately for each stream frame with the `--per-frame` option:

```shell
$ ./jelly-cli rdf inspect input.jelly --per-frame
```

In both cases, you will get the output as a valid YAML.

### Validate Jelly files

To validate a Jelly file, run

```shell
$ ./jelly-cli rdf validate input.jelly
```

You can also check whether the Jelly file has been encoded using specific stream options or is equivalent to another RDF file, with the use of additional options to this command.

### General tips

Use the `--help` option to learn more about all the available settings:

```shell
$ ./jelly-cli rdf to-jelly --help
$ ./jelly-cli rdf from-jelly --help
$ ./jelly-cli rdf inspect --help
$ ./jelly-cli rdf validate --help
```

And use the `--debug` option to get more information about any exceptions you encounter. 
You can set it for any of the commands described above, for example:

```shell
$ ./jelly-cli rdf to-jelly --debug
```

For more information, go to the official [jelly-cli documentation](https://jelly-rdf.github.io/dev/use-cases/jelly-cli).

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
