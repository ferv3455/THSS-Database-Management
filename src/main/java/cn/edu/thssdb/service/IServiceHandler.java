package cn.edu.thssdb.service;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.*;
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
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StatusUtil;
import org.apache.thrift.TException;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    long sessionId = req.getSessionId();
    LogicalPlan plan = null;
    try {
      plan = LogicalGenerator.generate(req.statement);
    } catch (Exception e) {
      e.printStackTrace();
      return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
    }

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
        try {
          InsertPlan ins_plan = (InsertPlan) plan;
          String name = ins_plan.getTableName();
          // TODO: session
          String[] columns = ins_plan.getColumns();
          for (String[] values : ins_plan.getValues())
          {
            System.out.printf("%s %s %s\n", name, Arrays.toString(columns), Arrays.toString(values));
            manager.getCurrent().insert(name, columns, values);
          }
          return new ExecuteStatementResp(
                  StatusUtil.success(String.format("%d row(s) inserted.", ins_plan.getValues().size())), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case DELETE:
        try {
          DeletePlan del_plan = (DeletePlan) plan;
          String name = del_plan.getTableName();
          Logic logic = del_plan.getLogic();
          // TODO: session
          System.out.printf("%s %s\n", name, logic);
          String msg = manager.getCurrent().delete(name, logic);
          return new ExecuteStatementResp(StatusUtil.success(msg), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case UPDATE:
        try {
          UpdatePlan update_plan = (UpdatePlan) plan;
          String name = update_plan.getTableName();
          String columnName = update_plan.getColumnName();
          Comparer value = update_plan.getValue();
          Logic logic = update_plan.getLogic();
          // TODO: session
          System.out.printf("%s %s\n", name, logic);
          String msg = manager.getCurrent().update(name, columnName, value, logic);
          return new ExecuteStatementResp(StatusUtil.success(msg), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case SELECT:
        try {
          SelectPlan sel_plan = (SelectPlan) plan;
          List<Pair<String, String>> resultColumns = sel_plan.getResultColumns();
          List<Pair<List<String>, Logic>> tableQuery = sel_plan.getTableQuery();
          QueryTable queryTable = sel_plan.getTable();
          Logic logic = sel_plan.getLogic();

          // TODO: session
          System.out.printf("%s %s %s\n", resultColumns, tableQuery, logic);
          QueryResult result = manager.getCurrent().select(resultColumns, queryTable, logic);

          // Show query result
          ExecuteStatementResp resp = new ExecuteStatementResp(StatusUtil.success(), true);
          for (String columnName : result.getColumnNames()) {
            resp.addToColumnsList(columnName);
          }
          while (result.hasNext()) {
            Row row = result.next();
            if (row == null) {
              break;
            }
            ArrayList<Entry> entries = row.getEntries();
            resp.addToRowList(entries.stream().map(Entry::toString).collect(Collectors.toList()));
          }
          if (resp.getRowListSize() == 0) {
            return new ExecuteStatementResp(StatusUtil.success("Empty set."), false);
          }
          return resp;
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      default:
        System.out.println("[DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success(), false);
    }
  }
}
