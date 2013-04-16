
pushd `dirname $0`
 
hadoop fs -rm workflow/workflow.xml && hadoop fs -put workflow.xml workflow
oozie job -oozie http://localhost:11000/oozie -config job.properties -run

popd
