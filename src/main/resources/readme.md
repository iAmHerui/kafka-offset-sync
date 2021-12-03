1 工程的pom文件中添加依赖
<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>connect-mirror</artifactId>
	<version>2.7.0</version>
	<exclusions>
		<exclusion>
			<groupId>org.apache.kafka</groupId>
			<artifactId>kafka-clients</artifactId>
		</exclusion>
	</exclusions>
</dependency>
<dependency>
	<groupId>org.apache.kafka</groupId>
	<artifactId>kafka-clients</artifactId>
	<version>2.3.0</version>
</dependency>
2 方法入口：Test.java中的main方法，方法中调用了sourceToTargetCommmitOffsetTest方法，此方法先从目的端获取来自源端同步的消费者组的消费位移，然后提交此位移到目的集群
