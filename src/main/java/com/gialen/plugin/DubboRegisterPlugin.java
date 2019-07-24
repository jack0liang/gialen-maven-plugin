package com.gialen.plugin;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Component;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Mojo(name="dubbo-append")
public class DubboRegisterPlugin extends AbstractMojo {
    @Parameter
    String dubboPkg;
    @Parameter
    HashMap<String,String> serviceKeyPrefix;
    @Parameter(defaultValue = "jdbc:mysql://172.30.2.183:3306/gialen_admin?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true")
    String jdbcUrl;
    @Parameter(defaultValue = "dev")
    String jdbcUser;
    @Parameter(defaultValue = "devtest")
    String jdbcPassword;
    @Component
    MavenProject project;

    public void execute() throws MojoExecutionException, MojoFailureException {
        Map<String,String> keyPrefixMap = getKeyPrefix();
        if(keyPrefixMap==null||keyPrefixMap.isEmpty()) {
            getLog().warn("can't found interface class");
            return;
        }
        String path = project.getBuild().getOutputDirectory();
        //System.out.println(path);
        Connection conn = null;
        try {
            URL classes = new URL("file://" + path + "/");
            ClassLoader custom = new URLClassLoader(new URL[]{classes}, getClass().getClassLoader());
            conn = DriverManager.getConnection(jdbcUrl,jdbcUser,jdbcPassword);
            for (Map.Entry<String,String> interfaceClz : keyPrefixMap.entrySet()) {
                Class clz = custom.loadClass(interfaceClz.getValue());
                if(clz.isInterface()){
                    registerInterface(clz,conn,interfaceClz.getKey());
                }
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

    private Map<String,String> getKeyPrefix(){
        if(serviceKeyPrefix!=null&&serviceKeyPrefix.size()>0)
            return serviceKeyPrefix;
        return PkgUtils.getClassByPkg(dubboPkg,project);
    }

    String findByClzName = "select * from dubbo_interface where interface_name=? and method=?";
    String findMethodParam = "select * from dubbo_method_param where service_key=? order by position";
    String addDubboInterface = "insert into dubbo_interface(service_key,interface_name,method,update_by)values(?,?,?,0)";
    String delMethodParam = "delete from dubbo_method_param where service_key=?";
    String addMethodParam = "insert into dubbo_method_param(service_key,type,position,param_name) values (?,?,?,?)";

    private void registerInterface(Class clz, Connection conn, String keyPrefix) throws SQLException {
        getLog().info("process "+clz.getName());
        String clzName = clz.getName();
        PreparedStatement pstmt = conn.prepareStatement(findByClzName);
        Method[] methods = clz.getMethods();
        for(Method method:methods){
            String methodName = method.getName();
            pstmt.setString(1,clzName);
            pstmt.setString(2,methodName);
            ResultSet rs = pstmt.executeQuery();
            String serviceKey = null;
            if(rs.next()){
                int id = rs.getInt("id");
                serviceKey = rs.getString("service_key");
            }else{
                serviceKey = keyPrefix+"."+methodName;
                //插入服务接口记录
                PreparedStatement ipstmt = conn.prepareStatement(addDubboInterface);
                ipstmt.setString(1,serviceKey);
                ipstmt.setString(2,clzName);
                ipstmt.setString(3,methodName);
                ipstmt.execute();
                getLog().info("append to mysql:"+serviceKey);
            }
            registerMethodParam(clz,method,serviceKey,conn);
        }
    }

    private void registerMethodParam(Class clz, Method method,String serviceKey, Connection conn) throws SQLException {
        getLog().info("process method param:"+clz.getName()+" "+method.getName());
        PreparedStatement pstmt = conn.prepareStatement(findMethodParam);
        pstmt.setString(1,serviceKey);
        ResultSet rs = pstmt.executeQuery();
        List<String> paramTypeList = new ArrayList<>();
        while (rs.next()){
            String type = rs.getString("type");
            paramTypeList.add(type);
            //getLog().info(rs.getString("type")+","+rs.getInt("position")+","+rs.getString("param_name"));
        }
        java.lang.reflect.Parameter[] parameters = method.getParameters();
        if(parameters.length!=paramTypeList.size()) {
            deleteMethodParam(conn, serviceKey);
        } else {
            boolean isDelete = false;
            for (int i = 0; i < parameters.length; i++) {
                java.lang.reflect.Parameter parameter = parameters[i];
                if (parameter.getType().getName().equals(paramTypeList.get(i))) {
                    isDelete = true;
                    break;
                }
            }
            if (isDelete)
                deleteMethodParam(conn, serviceKey);
        }
        if(parameters.length==0)
            return ;
        PreparedStatement ipstmt = conn.prepareStatement(addMethodParam);
        for(int i=0;i<parameters.length;i++){
            java.lang.reflect.Parameter parameter = parameters[i];
            ipstmt.setString(1,serviceKey);
            String paramType = parameter.getType().getName();
            ipstmt.setString(2,paramType);
            ipstmt.setInt(3,i);
            String paramName = parameter.getName();
            if("userId".equals(paramName))
                paramName = "sessionUserId";
            else if(parameters.length==1&&paramType.startsWith("com.gialen."))
                paramName = "";
            ipstmt.setString(4,paramName);
            ipstmt.addBatch();
            getLog().info(serviceKey+" add method param:"+paramType+" "+paramName+";position:"+i);
        }
        ipstmt.executeBatch();
    }

    private void deleteMethodParam(Connection conn, String serviceKey) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement(delMethodParam);
        pstmt.setString(1,serviceKey);
        pstmt.execute();
        getLog().info("delete method param:"+serviceKey);
    }

    public void setServiceKeyPrefix(HashMap<String, String> serviceKeyPrefix) {
        this.serviceKeyPrefix = serviceKeyPrefix;
    }
}
