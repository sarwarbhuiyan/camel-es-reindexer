package com.sarwarbhuiyan.tools.elasticsearch;

import java.util.concurrent.CountDownLatch;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ShutdownRunningTask;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

import com.sarwarbhuiyan.camel.component.elasticsearch.http.BulkIndexStrategy;
import com.sarwarbhuiyan.camel.component.elasticsearch.http.BulkReindexStrategy;

public class ReindexRouteBuilder extends RouteBuilder {

	private int bulkSize = 500;
	private int outputWorkers = 2;
	private boolean preserveIDs = false;
	private String scrollPeriod = "1m";
	private String sourceHost = "localhost";
	private String sourcePort = "9200";
	private String targetHost = "localhost";
	private String targetPort = "9200";
	private String sourceIndex = null;
	private String targetIndex = null;
	private Object scrollSize;
	private String scanQuery;
	
	private Main main;
	
	public int getBulkSize() {
		return bulkSize;
	}

	public void setBulkSize(int bulkSize) {
		this.bulkSize = bulkSize;
	}

	public int gtetOutputWorkers() {
		return outputWorkers;
	}

	public void setOutputWorkers(int outputWorkers) {
		this.outputWorkers = outputWorkers;
	}

	public boolean isPreserveIDs() {
		return preserveIDs;
	}

	public void setPreserveIDs(boolean preserveIDs) {
		this.preserveIDs = preserveIDs;
	}

	public String getScrollPeriod() {
		return scrollPeriod;
	}

	public void setScrollPeriod(String scrollPeriod) {
		this.scrollPeriod = scrollPeriod;
	}

	public String getSourceHost() {
		return sourceHost;
	}

	public void setSourceHost(String sourceHost) {
		this.sourceHost = sourceHost;
	}

	public String getSourcePort() {
		return sourcePort;
	}

	public void setSourcePort(String sourcePort) {
		this.sourcePort = sourcePort;
	}

	public String getTargetHost() {
		return targetHost;
	}

	public void setTargetHost(String targetHost) {
		this.targetHost = targetHost;
	}

	public String getTargetPort() {
		return targetPort;
	}

	public void setTargetPort(String targetPort) {
		this.targetPort = targetPort;
	}

	public String getSourceIndex() {
		return sourceIndex;
	}

	public void setSourceIndex(String sourceIndex) {
		this.sourceIndex = sourceIndex;
	}

	public String getTargetIndex() {
		return targetIndex;
	}

	public void setTargetIndex(String targetIndex) {
		this.targetIndex = targetIndex;
	}
	
	public void setMain(Main main) {
		this.main = main;
	}

	public ReindexRouteBuilder(String sourceIndex, String targetIndex) {
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
	}

	@Override
	public void configure() throws Exception {
		StringBuilder sourceRouteOptionsBuilder = new StringBuilder();
		sourceRouteOptionsBuilder.append("eshttp://elasticsearch?")
								 .append("ip=").append(this.sourceHost).append("&")
								 .append("port=").append(this.sourcePort).append("&")
								 .append("operation=SCAN_SCROLL").append("&")
								 .append("indexName=").append(this.sourceIndex).append("&")
								 .append("scrollSize=").append(this.bulkSize).append("&")
								 .append("scrollPeriod=").append(this.scrollPeriod);
		if(this.scanQuery!=null)
			sourceRouteOptionsBuilder.append("&scanQuery=").append(this.scanQuery);
		
		StringBuilder targetRouteOptionsBuilder = new StringBuilder();
		targetRouteOptionsBuilder.append("eshttp://elasticsearch?")
								 .append("ip=").append(this.targetHost).append("&")
								 .append("port=").append(this.targetPort).append("&")
								 .append("operation=BULK_INDEX").append("&")
								 .append("preserveIds=").append(this.preserveIDs);
		if(this.targetIndex!=null)
			targetRouteOptionsBuilder.append("&indexName=").append(this.targetIndex!=null?this.targetIndex:"");

		from(sourceRouteOptionsBuilder.toString())
		.startupOrder(1)
		.shutdownRunningTask(ShutdownRunningTask.CompleteAllTasks)
		.choice()
			.when(body().isEqualTo("DONE"))
			    .process(new Processor() {
					public void process(Exchange exchange) throws Exception {
						main.completed();
					}
				})
			    .to("log:reindexer?level=INFO")
			.otherwise()	
			  .aggregate(constant(true), (this.preserveIDs?new BulkReindexStrategy():new BulkIndexStrategy()))
		      .completionSize(this.bulkSize)
		      .forceCompletionOnStop()
		      .completionTimeout(60000)
		      .to("seda:bulkRequests");
		
		from("seda:bulkRequests?concurrentConsumers="+this.outputWorkers)
		.startupOrder(2)
		.shutdownRunningTask(ShutdownRunningTask.CompleteAllTasks)
		.to(targetRouteOptionsBuilder.toString());
		
	}
	
	public String getScanQuery(String scanQuery) {
		return this.scanQuery;
	}

	public void setScanQuery(String scanQuery) {
		this.scanQuery = scanQuery;
	}

    
}
