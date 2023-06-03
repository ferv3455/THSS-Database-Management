package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class IServiceHandler implements IService.Iface {

  private static final AtomicInteger sessionCnt = new AtomicInteger(0);

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    return new ConnectResp(StatusUtil.success(), sessionCnt.getAndIncrement());
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws TException {
    return new DisconnectResp(StatusUtil.success());
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    if (req.getSessionId() < 0) {
      return new ExecuteStatementResp(
          StatusUtil.fail("You are not connected. Please connect first."), false);
    }
    // TODO: implement execution logic
    Manager manager = Manager.getInstance();
    LogicalPlan plan = LogicalGenerator.generate(req.statement);
    switch (plan.getType()) {
      case CREATE_DB:
        try {
          String name = ((CreateDatabasePlan) plan).getDatabaseName();
          manager.createDatabaseIfNotExists(name);
          manager.persistdb(name);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Created database %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case DROP_DB:
        try {
          String name = ((DropDatabasePlan) plan).getDatabaseName();
          manager.deleteDatabase(name);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Dropped database %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case USE_DB:
        try {
          String name = ((UseDatabasePlan) plan).getDatabaseName();
          manager.switchDatabase(name);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Database changed to %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case SHOW_DB:
        try {
          ExecuteStatementResp resp =
              new ExecuteStatementResp(StatusUtil.success("Showing all databases."), true);
          resp.addToColumnsList("Database");
          for (String name : manager.getAll()) {
            resp.addToRowList(Collections.singletonList(name));
          }
          return resp;
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case CREATE_TB:
        try {
          CreateTablePlan ct_plan = (CreateTablePlan) plan;
          String name = ct_plan.getTableName();
          Column[] columns = ct_plan.getColumns();
          manager.getCurrent().create(name, columns);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Created table %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case DROP_TB:
        try {
          String name = ((DropTablePlan) plan).getTableName();
          manager.getCurrent().drop(name);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Dropped table %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case SHOW_TB:
        try {
          String name = ((ShowTablePlan) plan).getTableName();
          ExecuteStatementResp resp =
              new ExecuteStatementResp(
                  StatusUtil.success(String.format("Showing table %s.", name)), true);
          resp.addToColumnsList("Field");
          resp.addToColumnsList("Type");
          resp.addToColumnsList("Null");
          resp.addToColumnsList("Key");
          for (Column column : manager.getCurrent().get(name).getColumns()) {
            resp.addToRowList(
                Arrays.asList(
                    column.getName(),
                    column.getType().toString(),
                    column.isNotNull() ? "YES" : "NO",
                    column.getPrimary() > 0 ? "PRI" : ""));
          }
          return resp;
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case INSERT:
      case DELETE:
      case UPDATE:
      case SELECT:
        System.out.println("[DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success(), false);
      default:
    }
    return null;
  }
}
