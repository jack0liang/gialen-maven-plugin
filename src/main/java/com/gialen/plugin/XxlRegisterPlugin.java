package com.gialen.plugin;

import com.gialen.xxljob.annotation.GialenJobInfo;
import com.xxl.job.core.handler.annotation.JobHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Map;

@Mojo(name="xxl-append")
public class XxlRegisterPlugin extends AbstractMojo {
    @Parameter
    String jobsPkg;
    @Parameter(defaultValue = "jdbc:mysql://172.30.2.183:3306/xxl-job?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true")
    String jdbcUrl;
    @Parameter(defaultValue = "dev")
    String jdbcUser;
    @Parameter(defaultValue = "devtest")
    String jdbcPassword;
    @Parameter
    String appName;
    @Parameter
    String appTitle;
    @Component
    MavenProject project;

    public void execute() throws MojoExecutionException, MojoFailureException {
        Map<String,String> keyPrefixMap = PkgUtils.getClassByPkg(jobsPkg,project);
        if(keyPrefixMap==null||keyPrefixMap.isEmpty()) {
            getLog().warn("can't found job class");
            return;
        }
        String path = project.getBuild().getOutputDirectory();
        Connection conn = null;
        try {
            URL classes = new URL("file://" + path + "/");
            ClassLoader custom = new URLClassLoader(new URL[]{classes}, getClass().getClassLoader());
            conn = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
            int groupId = processGroupInfo(conn);
            for (Map.Entry<String,String> interfaceClz : keyPrefixMap.entrySet()) {
                Class clz = custom.loadClass(interfaceClz.getValue());
                processJobInfo(clz,conn,groupId);
            }
        }catch (Exception e){
            getLog().error(e.getMessage(),e);
        }finally {
            if(conn!=null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    getLog().error(e.getMessage(),e);
                }
            }
        }
    }

    String findByBeanName = "select * from xxl_job_qrtz_trigger_info where executor_handler=? and job_group=?";
    String insertJobInfo = "insert into xxl_job_qrtz_trigger_info(job_group,job_cron,job_desc,author,alarm_email,executor_route_strategy,executor_handler,executor_block_strategy,executor_timeout,executor_fail_retry_count,glue_type,glue_updatetime)" +
            " values(?,?,?,?,?,?,?,?,?,?,?,now())";
    String updateJobInfo = "update xxl_job_qrtz_trigger_info set job_cron=?,executor_route_strategy=?,executor_block_strategy=?,executor_timeout=?,executor_fail_retry_count=? where id=?";

    private void processJobInfo(Class clz, Connection conn, int groupId) throws SQLException {
        getLog().info("process class:"+clz.getName());
        JobHandler jobHandler = (JobHandler) clz.getAnnotation(JobHandler.class);
        if(jobHandler==null)
            return ;
        String beanName = jobHandler.value();
        if(StringUtils.isBlank(beanName))
            return ;
        GialenJobInfo jobInfo = (GialenJobInfo)clz.getAnnotation(GialenJobInfo.class);
        if(jobInfo==null)
            return ;
        PreparedStatement pstmt = conn.prepareStatement(findByBeanName);
        pstmt.setString(1,beanName);
        pstmt.setInt(2,groupId);
        ResultSet rs = pstmt.executeQuery();
        if(rs.next()){
            getLog().info("update job["+beanName+"]");
            int id = rs.getInt("id");
            if(id<=0)
                return ;
            PreparedStatement ipstmt = conn.prepareStatement(updateJobInfo);
            ipstmt.setString(1,jobInfo.cron());
            ipstmt.setString(2,jobInfo.executorRouteStrategy().name());
            ipstmt.setString(3,jobInfo.executorBlockStrategy().name());
            ipstmt.setInt(4,jobInfo.executorTimeout());
            ipstmt.setInt(5,jobInfo.executorFailRetryCount());
            ipstmt.setInt(6,id);
            ipstmt.execute();
        }else {
            getLog().info("add job["+beanName+"]");
            PreparedStatement ipstmt = conn.prepareStatement(insertJobInfo);
            ipstmt.setInt(1,groupId);
            ipstmt.setString(2,jobInfo.cron());
            String desc = jobInfo.desc();
            if(StringUtils.isBlank(desc))
                desc = beanName;
            ipstmt.setString(3,desc);
            ipstmt.setString(4,jobInfo.author());
            ipstmt.setString(5,jobInfo.alarmEmail());
            ipstmt.setString(6,jobInfo.executorRouteStrategy().name());
            ipstmt.setString(7,beanName);
            ipstmt.setString(8,jobInfo.executorBlockStrategy().name());
            ipstmt.setInt(9,jobInfo.executorTimeout());
            ipstmt.setInt(10,jobInfo.executorFailRetryCount());
            ipstmt.setString(11,jobInfo.glueType().name());
            ipstmt.execute();
        }
    }


    String findGroupInfo = "select * from xxl_job_qrtz_trigger_group where app_name=?";
    String insertGroupInfo = "insert into xxl_job_qrtz_trigger_group(id,app_name,title,`order`) values(?,?,?,?)";
    //获取刚插入的自增长id的值
    String findMaxId = "select max(id) from xxl_job_qrtz_trigger_group";
    private int processGroupInfo(Connection conn) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(findGroupInfo);
        pstmt.setString(1,appName);
        ResultSet rs = pstmt.executeQuery();
        if(rs.next()){
            int id = rs.getInt("id");
            return id;
        }else {
            int maxId = getGroupMaxId(conn);
            int id = maxId+1;
            PreparedStatement ipstmt = conn.prepareStatement(insertGroupInfo);
            ipstmt.setInt(1,id);
            ipstmt.setString(2,appName);
            if(StringUtils.isBlank(appTitle))
                appTitle = appName.substring(0,appName.length()>12?11:appName.length()-1);
            ipstmt.setString(3,appTitle);
            ipstmt.setInt(4,id);
            ipstmt.execute();
            return id;
        }
    }

    private int getGroupMaxId(Connection conn) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(findMaxId);
        ResultSet rs = pstmt.executeQuery();
        if(rs.next()){
            int id = rs.getInt(1);
            return id;
        }else {
            return 0;
        }
    }
}
