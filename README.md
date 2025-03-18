# jelly-cli

***Warning: This project is in early development and is not yet ready for production use.***

CLI utility for working with Jelly data.

## Quick start

Download the latest binary release from the [releases page](https://github.com/Jelly-RDF/cli/releases/tag/dev).

Run it like so:

```shell
$ ./jelly-cli --help
```

### Supported platforms

- Linux (x86_64, ARM64)
- macOS (ARM64)
- Windows (x86_64)

## Building from source

- Ensure you have [GraalVM](https://www.graalvm.org/) installed and the `native-image` utility is available in your `PATH`.
- Clone the repository.
- Run `sbt GraalVMNativeImage/packageBin`
- The binary will be available at `./target/graalvm-native-image/jelly-cli`.

Alternatively, you can use the utility with your JVM (no ahead-of-time compilation), by running `sbt run`. 
