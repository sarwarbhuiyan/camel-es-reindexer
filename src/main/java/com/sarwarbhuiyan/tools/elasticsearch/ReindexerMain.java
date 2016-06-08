package com.sarwarbhuiyan.tools.elasticsearch;

import org.apache.camel.main.Main;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ReindexerMain {

	public static void main(String[] args) {
		
		CommandLineParser parser = new DefaultParser();
		
		Options options = new Options();
		options.addOption(Option.builder().argName("sourceHost").hasArg().desc("Source Elasticsearch Host").longOpt("sourceHost").build());
		options.addOption(Option.builder().argName("sourcePort").hasArg().desc("Source Elasticsearch Port").longOpt("sourcePort").build());
		options.addOption(Option.builder().argName("targetHost").hasArg().desc("Target Elasticsearch Host").longOpt("targetHost").build());
		options.addOption(Option.builder().argName("targetPort").hasArg().desc("Target Elasticsearch Port").longOpt("targetPort").build());
		options.addOption(Option.builder().argName("sourceIndex").hasArg().desc("Source Index").longOpt("sourceIndex").required().build());
		options.addOption(Option.builder().argName("targetIndex").hasArg().desc("Target Index").longOpt("targetIndex").required().build());
		options.addOption(Option.builder().argName("preserveIDs").hasArg(false).desc("Preserve IDs?").longOpt("preserveIDs").build());
		options.addOption(Option.builder().argName("bulkSize").hasArg().desc("Bulk Size (default: 500)").longOpt("bulkSize").build());
		options.addOption(Option.builder().argName("scrollPeriod").hasArg().desc("Scroll Period (default: 1m)").longOpt("scrollPeriod").build());
		options.addOption(Option.builder().argName("outputWorkers").hasArg().desc("Output Workers (default: 2)").longOpt("outputWorkers").build());
		
		try {
			if(args.length < 1) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.printHelp( "es-reindexer", options );
			}
			CommandLine line = parser.parse(options, args);
			String sourceIndex = line.getOptionValue("sourceIndex");
			String targetIndex = line.getOptionValue("targetIndex");
			ReindexRouteBuilder reindexRouteBuilder = new ReindexRouteBuilder(sourceIndex, targetIndex);
			if(line.hasOption("sourceHost"))
				reindexRouteBuilder.setSourceHost(line.getOptionValue("sourceHost"));
			if(line.hasOption("sourcePort"))
				reindexRouteBuilder.setSourcePort(line.getOptionValue("sourcePort"));
			if(line.hasOption("targetHost"))
				reindexRouteBuilder.setTargetHost(line.getOptionValue("targetHost"));
			if(line.hasOption("targetPort"))
				reindexRouteBuilder.setTargetPort(line.getOptionValue("targetPort"));
			if(line.hasOption("preserveIDs"))
				reindexRouteBuilder.setPreserveIDs(true);
			if(line.hasOption("bulkSize")) {
				int bulkSize = Integer.parseInt(line.getOptionValue("bulkSize"));
				reindexRouteBuilder.setBulkSize(bulkSize);
			}
			if(line.hasOption("scrollPeriod"))
				reindexRouteBuilder.setScrollPeriod(line.getOptionValue("scrollPeriod"));
			if(line.hasOption("outputWorkers"))
				reindexRouteBuilder.setOutputWorkers(Integer.parseInt(line.getOptionValue("outputWorkers")));
			
			Main main = new Main();
			main.addRouteBuilder(reindexRouteBuilder);
			main.run();
			
			
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println("Unexpected exception: " + e.getMessage());
		} catch (NumberFormatException e) {
			System.out.println("Number expected, found non-numeric string instead");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
