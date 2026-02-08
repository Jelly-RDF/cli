[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Discord](https://img.shields.io/discord/1333391881404420179?label=Discord%20chat)](https://discord.gg/A8sN5XwVa5) [![Scala build and test](https://github.com/Jelly-RDF/cli/actions/workflows/scala.yml/badge.svg)](https://github.com/Jelly-RDF/cli/actions/workflows/scala.yml) [![GitHub Downloads (all assets, all releases)](https://img.shields.io/github/downloads/Jelly-RDF/cli/total)](https://github.com/Jelly-RDF/cli/releases) [![GitHub Action Marketplace](https://img.shields.io/badge/GitHub%20Action-Marketplace-blue)](https://github.com/marketplace/actions/setup-jelly-cli)

# jelly-cli

Fast and convenient CLI utility for working with the [Jelly](https://jelly-rdf.github.io/dev/) knowledge graph streaming protocol.

## ⚡ Quick start

### Install script

If you are using Linux (x86_64, ARM64), macOS (ARM64), or WSL on Windows, the recommended way is to use the [`setup-cli.sh`](https://github.com/Jelly-RDF/cli/blob/main/install.sh) script, which will automatically install the newest release.
To do so, run:

```shell
. <(curl -sSfL https://w3id.org/jelly/setup-cli.sh)
jelly-cli
```

### [mise](https://mise.jdx.dev/getting-started.html)

You can install `jelly-cli` on any platform (including Windows) using [mise](https://mise.jdx.dev/getting-started.html). Simply run:

```shell
mise use -g 'ubi:Jelly-RDF/cli[exe=jelly-cli]'
jelly-cli
```

### Manual download

Alternatively, you can download the binary manually (this also works for Windows). Go to the **[releases page](https://github.com/Jelly-RDF/cli/releases/latest)** and download the build for your platform.

You can then run it like so:

```shell
chmod +x jelly-cli
./jelly-cli
```

Or, on Windows:

```shell
jelly-cli.exe
```

## Usage

### Convert RDF to Jelly

To convert an RDF file (e.g., Turtle) to Jelly, simply run:

```shell
jelly-cli rdf to-jelly input.ttl > output.jelly
```

### Convert Jelly to RDF

To convert from Jelly to RDF run:

```shell
jelly-cli rdf from-jelly input.jelly > output.nq
```

By default, `jelly-cli` will translate files to NQuads. 
You can also specify the output format with `--out-format`, for example:

```shell
jelly-cli rdf from-jelly input.jelly --out-format=ttl > output.ttl
```

You can specify most well-known formats supported by Apache Jena, but also the custom `jelly-text` format. 
`jelly-text` is a human-readable translation of Jelly binary. It's not meant for machine consumption. It is useful for debugging and inspecting Jelly files.

### Transcode Jelly files

The `rdf transcode` command turns one or more input Jelly streams into a single output stream. It's extremely fast, using a dedicated transcoding algorithm. This algorithm has a limitation: the lookup sizes of the output stream must be greater than or equal to the lookup sizes of the input streams.

```shell
jelly-cli rdf transcode input.jelly > output.jelly
```

### Inspect Jelly files

To inspect a Jelly file and get basic information describing its contents, such as stream options or number of triples in the file, run:

```shell
jelly-cli rdf inspect input.jelly
```

You can also compute the statistics separately for each stream frame with the `--per-frame` option:

```shell
jelly-cli rdf inspect input.jelly --per-frame
```

In both cases, you will get the output as a valid YAML.

### Validate Jelly files

To validate a Jelly file, run:

```shell
jelly-cli rdf validate input.jelly
```

You can also check whether the Jelly file has been encoded using specific stream options or is equivalent to another RDF file, with the use of additional options to this command.

### General tips

Use the `--help` option to learn more about all the available settings:

```shell
jelly-cli rdf to-jelly --help
jelly-cli rdf from-jelly --help
jelly-cli rdf transcode --help
jelly-cli rdf inspect --help
jelly-cli rdf validate --help
```

And use the `--debug` option to get more information about any exceptions you encounter.

## Alternative installation: JAR builds

If for some reason the binaries wouldn't work for you, or you want to get better performance for large files (see *Performance considerations* below), you can use the JAR build. This build runs on any platform, as long as you have Java (min. version 17). Go to the **[releases page](https://github.com/Jelly-RDF/cli/releases/latest)** and download the `jelly-cli.jar` file. Then, run it like so:

```shell
java -jar jelly-cli.jar --help
```

For simple day-to-day use cases, we recommend using the binary distribution, because it has way faster startup times and doesn't require you to install Java.

### Performance considerations

If you are bulk-converting large amounts of RDF data (>10M triples), you may want to use the JAR build instead of the pre-compiled binary. With the JAR, your JVM will perform just-in-time compilation, resulting in better optimized code. The application will take longer to start, but the overall throughput will be better for large files.

For maximum performance, we recommend using a recent JVM (e.g., OpenJDK 25). In some cases we saw up to 2x better throughput when running the JAR with GraalVM 24 in JIT mode, as compared to the pre-compiled binary.

## GitHub Action

If you're using `jelly-cli` in your GitHub Action CI/CD workflows, consider using the `jelly-rdf/setup-cli` action ([Marketplace link](https://github.com/marketplace/actions/setup-jelly-cli)). The action will automatically download and install the appropriate binary. Simply run the action before `jelly-cli` usage:

```yaml
steps:
  - uses: Jelly-RDF/setup-cli@v1
    
  - run: jelly-cli rdf to-jelly input.ttl > output.jelly
```

## RDF-star/RDF 1.2 compatibility

`jelly-cli` is based on [Apache Jena](https://jena.apache.org/) 5.3.0, which is the last version of Jena that supports RDF-star. Later versions removed RDF-star support in favor of draft support for RDF 1.2, which is not directly compatible with RDF-star. Because RDF 1.2 is not yet a W3C Recommendation, we stick to RDF-star for now and will update to RDF 1.2 once it is finalized.

## Contributing

We welcome contributions! Feel free to [open an issue](https://github.com/Jelly-RDF/cli/issues/new), suggest new features, or submit pull requests.

💬 [**Chat with us on Discord**](https://discord.gg/A8sN5XwVa5) to ask questions or discuss ideas.

⭐ **Consider starring this repository** – this really helps the project!

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

----

The development of the Jelly protocol, its implementations, and supporting tooling was co-funded by the European Union. **[More details](https://w3id.org/jelly/dev/licensing/projects)**.
