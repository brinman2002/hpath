package io.github.brinman2002.hpath.command;

import io.github.brinman2002.hpath.pipeline.HPath;

import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.NotImplementedException;
import org.apache.crunch.PCollection;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableMap;

public class HPathCommand {

    final ImmutableMap<String, PType<?>> types = ImmutableMap.<String, PType<?>> builder().put("string", Avros.strings()).put("int", Avros.ints())
            .put("double", Avros.doubles()).put("long", Avros.longs()).put("float", Avros.floats()).build();

    public void run(final CommandLine commandLine) {

        // TODO validate arguments

        final PType<?> outputType;
        if (commandLine.hasOption('r')) {
            outputType = types.get(commandLine.getOptionValue('r'));
            if (outputType == null) {
                throw new IllegalArgumentException("Unknown output type of " + types.get(commandLine.getOptionValue('r')));
            }
        } else if (commandLine.hasOption("rf")) {
            throw new NotImplementedException("Feature not implemented");
        } else {
            throw new IllegalArgumentException("Please select a valid output schema or type option.");
        }

        final String query = commandLine.getOptionValue('q');

        final Path input = new Path(commandLine.getOptionValue('i'));

        final Pipeline pipeline = new MRPipeline(this.getClass());

        System.out.println(pipeline.getConfiguration());
        final PCollection<Record> collection = pipeline.read(From.avroFile(input));
        final PCollection<?> result = HPath.query(collection, query, outputType);

        pipeline.run();
        pipeline.done();

        // TODO validate that some kind of output is specified?

        if (commandLine.hasOption('o')) {
            final Path output = new Path(commandLine.getOptionValue('o'));
            result.write(To.avroFile(output));
        }

        if (commandLine.hasOption('c')) {
            System.out.println("output:");
            for (final Object o : result.materialize()) {
                System.out.println(o);
            }
        }

    }

    public static void main(final String[] args) throws Exception {
        final GnuParser parser = new GnuParser();
        final CommandLine commandLine = parser.parse(getOptions(), args);
        if (commandLine.hasOption('h')) {
            new HelpFormatter().printHelp("TODO", getOptions());
            return;
        }

        new HPathCommand().run(commandLine);
    }

    static Options getOptions() {
        final Options options = new Options();

        options.addOption("r", "result-type", true, "The expected result type. Acceptable values are 'string', 'int' and 'double'.");
        options.addOption("s", "schema", true, "The literal Avro schema definition for the output.");
        options.addOption("sf", "schema-file", true, "The literal Avro schema definition for the output.");

        options.addOption("i", "input", true, "Input path");
        options.addOption("o", "output", true, "Output path");
        options.addOption("q", "query", true, "A supported XPath query. Please see documentation for more information on what XPath is supported.");

        options.addOption("c", "console", false, "In addition to the output path, also materialize the results and output them to the console.");
        options.addOption("h", "help", false, "This help");
        return options;
    }
}
