package com.gialen.plugin;

import org.apache.commons.lang3.StringUtils;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class PkgUtils {

    public static Map<String, String> getClassByPkg(String pkg, MavenProject project) {
        if(StringUtils.isBlank(pkg))
            throw new RuntimeException("parameter serviceKeyPrefix and pkg must has one");
        String dubboIntefacePath = pkg.replace(".", File.separator);
        String path = project.getBuild().getOutputDirectory();
        File interfaceDir = new File(path + File.separator + dubboIntefacePath);
        Map<String,String> clzMap = new HashMap<>();
        for (File f : interfaceDir.listFiles()) {
            String clzName = f.getName();
            if (clzName.endsWith(".class")) {
                String clzSimpleName = clzName.replace(".class", "");
                clzMap.put(clzSimpleName, pkg + "." + clzSimpleName);
            }
        }
        return clzMap;
    }
}
