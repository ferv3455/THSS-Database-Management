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
import cn.edu.thssdb.schema.*;
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
    try {
      long sessionId = req.getSessionId();
      Manager.getInstance().quit(sessionId);
      return new DisconnectResp(StatusUtil.success());
    } catch (Exception e) {
      return new DisconnectResp(StatusUtil.fail(e.toString()));
    }
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
          manager.createDatabaseIfNotExists(name, sessionId);
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
          manager.switchDatabase(name, sessionId);
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
          Database database = manager.getCurrent(sessionId);
          CreateTablePlan ct_plan = (CreateTablePlan) plan;
          String name = ct_plan.getTableName();
          Column[] columns = ct_plan.getColumns();
          database.create(name, columns);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Created table %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case DROP_TB:
        try {
          Database database = manager.getCurrent(sessionId);
          String name = ((DropTablePlan) plan).getTableName();
          database.drop(name);
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("Dropped table %s.", name)), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case SHOW_TB:
        try {
          Database database = manager.getCurrent(sessionId);
          String name = ((ShowTablePlan) plan).getTableName();
          ExecuteStatementResp resp =
              new ExecuteStatementResp(
                  StatusUtil.success(String.format("Showing table %s.", name)), true);
          resp.addToColumnsList("Field");
          resp.addToColumnsList("Type");
          resp.addToColumnsList("Null");
          resp.addToColumnsList("Key");
          for (Column column : database.get(name).getColumns()) {
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
          Database database = manager.getCurrent(sessionId);
          InsertPlan ins_plan = (InsertPlan) plan;
          String tableName = ins_plan.getTableName();
          Table table = database.get(tableName);

          // Acquire x lock
          acquireXLock(manager, table, sessionId);

          // Perform insertion
          String[] columns = ins_plan.getColumns();
          for (String[] values : ins_plan.getValues()) {
            database.insert(tableName, columns, values);
          }

          // Free x lock if autocommit
          if (manager.transaction_sessions.contains(sessionId)) {
            table.freeXLock(sessionId);
          }

          // Response
          return new ExecuteStatementResp(
              StatusUtil.success(String.format("%d row(s) inserted.", ins_plan.getValues().size())),
              false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case DELETE:
        try {
          Database database = manager.getCurrent(sessionId);
          DeletePlan del_plan = (DeletePlan) plan;
          String tableName = del_plan.getTableName();
          Logic logic = del_plan.getLogic();
          Table table = database.get(tableName);

          // Acquire x lock
          acquireXLock(manager, table, sessionId);

          // Perform deletion
          String msg = database.delete(tableName, logic);

          // Free x lock if autocommit
          if (manager.transaction_sessions.contains(sessionId)) {
            table.freeXLock(sessionId);
          }

          // Response
          return new ExecuteStatementResp(StatusUtil.success(msg), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case UPDATE:
        try {
          Database database = manager.getCurrent(sessionId);
          UpdatePlan update_plan = (UpdatePlan) plan;
          String tableName = update_plan.getTableName();
          String columnName = update_plan.getColumnName();
          Comparer value = update_plan.getValue();
          Logic logic = update_plan.getLogic();
          Table table = database.get(tableName);

          // Acquire x lock
          acquireXLock(manager, table, sessionId);

          // Perform update
          String msg = database.update(tableName, columnName, value, logic);

          // Free x lock if autocommit
          if (manager.transaction_sessions.contains(sessionId)) {
            table.freeXLock(sessionId);
          }

          // Response
          return new ExecuteStatementResp(StatusUtil.success(msg), false);
        } catch (Exception e) {
          e.printStackTrace();
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case SELECT:
        try {
          Database database = manager.getCurrent(sessionId);
          SelectPlan sel_plan = (SelectPlan) plan;
          List<Pair<String, String>> resultColumns = sel_plan.getResultColumns();
          Pair<List<String>, Logic> tableQuery = sel_plan.getTableQuery().get(0);
          Logic logic = sel_plan.getLogic();

          // Construct query table
          QueryTable queryTable;
          if (tableQuery.left.size() == 1) {
            queryTable = database.buildSingleQueryTable(tableQuery.left.get(0));
          } else {
            queryTable = database.buildJointQueryTable(tableQuery.left, tableQuery.right);
          }

          // Acquire s lock
          List<String> tableNames = tableQuery.left;
          for (String tableName : tableNames) {
            acquireSLock(manager, database.get(tableName), sessionId);
          }

          // Perform query
          QueryResult result = database.select(resultColumns, queryTable, logic);

          // Free s lock if autocommit
          if (manager.transaction_sessions.contains(sessionId)) {
            for (String tableName : tableNames) {
              database.get(tableName).freeSLock(sessionId);
            }
          }

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

      case QUIT:
        try {
          manager.quit(sessionId);
          return new ExecuteStatementResp(StatusUtil.success(), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case BEGIN_TRANS:
        try {
          manager.getCurrent(sessionId);
          if (!manager.transaction_sessions.contains(sessionId)) {
            manager.transaction_sessions.add(sessionId);
            manager.s_lock_dict.put(sessionId, new ArrayList<>());
            manager.x_lock_dict.put(sessionId, new ArrayList<>());
          }
          return new ExecuteStatementResp(StatusUtil.success("Transaction begins."), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      case COMMIT:
        try {
          Database database = manager.getCurrent(sessionId);
          if (manager.transaction_sessions.contains(sessionId)) {
            manager.transaction_sessions.remove(sessionId);
            for (String tableName : manager.s_lock_dict.get(sessionId)) {
              Table table = database.get(tableName);
              table.freeSLock(sessionId);
              table.unpin();
            }
            for (String tableName : manager.x_lock_dict.get(sessionId)) {
              Table table = database.get(tableName);
              table.freeXLock(sessionId);
              table.freeSLock(sessionId);
              table.unpin();
            }
            manager.s_lock_dict.get(sessionId).clear();
            manager.x_lock_dict.get(sessionId).clear();
          }
          return new ExecuteStatementResp(StatusUtil.success("Transaction commited."), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.toString()), false);
        }

      default:
        System.out.println("[DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success(), false);
    }
  }

  public void acquireXLock(Manager manager, Table table, long sessionId) throws InterruptedException {
    // Add to queue
    if (!manager.session_queue_x.contains(sessionId)) {
      manager.session_queue_x.add(sessionId);
    }

    // Waiting until x lock is acquired
    while (true) {
      if (manager.session_queue_x.get(0) == sessionId) {
        // first in the queue: acquire the lock
        int lockX = table.getXLock(sessionId);
        int lockS = table.getSLock(sessionId); // get two locks at a time
        if (lockX >= 0 && lockS >= 0) {
          manager.x_lock_dict.computeIfAbsent(sessionId, k -> new ArrayList<>()).add(table.tableName);
          manager.session_queue_x.remove(0);
          break;
        }
        else {
          if (lockX >= 0) {
            table.freeXLock(sessionId);
          }
          if (lockS >= 0) {
            table.freeSLock(sessionId);
          }
        }
      }
      Thread.sleep(400);
    }
  }

  public void acquireSLock(Manager manager, Table table, long sessionId) throws InterruptedException {
    // Add to queue
    if (!manager.session_queue_s.contains(sessionId)) {
      manager.session_queue_s.add(sessionId);
    }

    // Waiting until s lock is acquired
    while (true) {
      if (manager.session_queue_s.get(0) == sessionId) {
        // first in the queue: acquire the lock
        int lock = table.getSLock(sessionId);
        if (lock >= 0) {
          manager.s_lock_dict.computeIfAbsent(sessionId, k -> new ArrayList<>()).add(table.tableName);
          manager.session_queue_s.remove(0);
          break;
        }
      }
      Thread.sleep(400);
    }
  }
}
