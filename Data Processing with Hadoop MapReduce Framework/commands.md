Commands to execute from Project Root Folder

**Create folder in hadoop file system**

	hadoop fs -mkdir InputFolder

**Upload dataset to the InputFolder**

	hadoop fs -copyFromLocal ./input/alice-1.txt

**View Uploaded file in the HDFS**

	hadoop fs -ls InputFolder

**Running MapReduce Operation**

	hadoop jar ./jars/assignment1.jar assignment1.Assignment1 InputFolder OutputFolder

**Copy OutputFolders from the HDFS**

	hadoop fs -copyToLocal OutputFolderWordCount

	hadoop fs -copyToLocal OutputFolderTop200

	hadoop fs -copyToLocal OutputFolderAvgWordLength

**To remote input folder from the HDFS**

	hadoop fs -rm -r InputFolder

**To remove output folder from the HDFS**

	hadoop fs -rm -r OutputFolderWordCount

	hadoop fs -rm -r OutputFolderTop200
	
	hadoop fs -rm -r OutputFolderAvgWordLength


