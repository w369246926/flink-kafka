package flink.test;

public class Test {
    public static void main(String[] args) throws Exception {
        //查看所有JOB
        //FlinkRestFulUtil.getAllJobMessage("123");

        //获取jodid
        //FlinkRestFulUtil.getAllJobMessage();

        //结束JOB  flinkJarRun
        FlinkRestFulUtil.flinkJarRun("123");
        //执行新JOB

        //FlinkRestFulUtil.getJarsMessage("123");
    }
}
