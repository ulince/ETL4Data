package com.udl.test;

import com.google.common.collect.Sets;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveProperties;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveRunnerSetup;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klarna.hiverunner.annotations.HiveSetupScript;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import org.apache.commons.collections.MapUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.HashMap;

@RunWith(StandaloneHiveRunner.class)
public class HiveTest {

    @HiveRunnerSetup
    public final HiveRunnerConfig CONFIG = new HiveRunnerConfig(){{
        setHiveExecutionEngine("mr");
    }};

    @HiveProperties
    public Map<String, String> properties = MapUtils.putAll(new HashMap(), new Object[]{
            "DIR", "${hadoop.tmp.dir}",
            "default.schema", "default",
    });

    @HiveSQL(files = {
            "ulincero/ETL/import.hql",
            "ulincero/ETL/audit.hql",
            "ulincero/ETL/load.hql"
    }, encoding = "UTF-8")
    private HiveShell shell;


    @Test
    public void osmTablesCreation() {
        HashSet<String> tablesExpected = Sets.newHashSet("osmnodes", "omsways",
            "osmrelations");
        HashSet<String> tablesCreated = Sets.newHashSet(hiveShell.executeQuery("show tables"));

        Assert.assertEquals(tablesExpected, tablesCreated);
    }

    @Test
    public void errorsTablesCreation() {
        HashSet<String> tablesExpected = Sets.newHashSet("errors", "stats");
        HashSet<String> tablesCreated = Sets.newHashSet(hiveShell.executeQuery("show tables"));

        Assert.assertEquals(tablesExpected, tablesCreated);
    }

    @Test
    public void osmInsertedRecords() {

        List<Object[]> dummy = shell.execute(files[0]);
        List<Object[]> result1 = shell.executeStatement("select count(*) from osmnodes");        
        List<Object[]> result2 = shell.executeStatement("select count(*) from osmways");
        List<Object[]> result3 = shell.executeStatement("select count(*) from osmrelations");
        assertTrue(result1.size() > 0);
        assertTrue(result2.size() > 0);
        assertTrue(result3.size() > 0);

    }

    @Test
    public void osmInsertedRecords() {

        List<Object[]> dummy = shell.execute(files[0]);
        List<Object[]> result1 = shell.executeStatement("select count(*) from osmnodes");        
        List<Object[]> result2 = shell.executeStatement("select count(*) from osmways");
        List<Object[]> result3 = shell.executeStatement("select count(*) from osmrelations");
        assertTrue(result1.size() > 0);
        assertTrue(result2.size() > 0);
        assertTrue(result3.size() > 0);

    }




}