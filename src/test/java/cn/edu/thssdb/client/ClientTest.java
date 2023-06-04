package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnectReq;
import cn.edu.thssdb.rpc.thrift.DisconnectResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.PrintStream;
import java.util.List;

public class ClientTest {
  private static long sessionID = -1;
  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static IService.Client client;

  public static void main(String[] args) {
    try {
      String host = Global.DEFAULT_SERVER_HOST;
      int port = Global.DEFAULT_SERVER_PORT;
      TTransport transport = new TSocket(host, port);
      transport.open();
      TProtocol protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      connect("root", "root");

      // TESTS BEGIN
      {
        String[] statement = {
          "create database test;\n",
          "use test;\n",
          "create table tb (id INT PRIMARY KEY, name STRING(8) NOT NULL);\n",
          "create table tb2 (id INT PRIMARY KEY, status STRING(8) NOT NULL);\n",
        };
        for (String st : statement) {
          ExecuteStatementReq req = new ExecuteStatementReq(sessionID, st);
          ExecuteStatementResp resp = client.executeStatement(req);
          printResp(resp);
        }
      }

      {
        long startTime = System.currentTimeMillis();
        String[] statement = {
          "insert into tb values (0, 'hi');\n",
          "insert into tb values (1, 'hello');\n",
          "insert into tb values (2, 'hello');\n",
          "insert into tb2 values (0, 'good'), (2, 'good'), (4, 'bad');\n",
        };
        for (String st : statement) {
          ExecuteStatementReq req = new ExecuteStatementReq(sessionID, st);
          ExecuteStatementResp resp = client.executeStatement(req);
          printResp(resp);
        }
        println("It costs " + (System.currentTimeMillis() - startTime) + "ms.");
      }

      {
        long startTime = System.currentTimeMillis();
        String[] statement = {
          "select tb.id, tb2.status from tb join tb2 on tb.id = tb2.id where tb.id > 1;\n",
        };
        for (String st : statement) {
          ExecuteStatementReq req = new ExecuteStatementReq(sessionID, st);
          ExecuteStatementResp resp = client.executeStatement(req);
          printResp(resp);
        }
        println("It costs " + (System.currentTimeMillis() - startTime) + "ms.");
      }

      {
        String[] statement = {
          "delete from tb;\n",
          "delete from tb2 where id > 2;\n",
          "drop table tb;\n",
          "drop database test;\n",
        };
        for (String st : statement) {
          ExecuteStatementReq req = new ExecuteStatementReq(sessionID, st);
          ExecuteStatementResp resp = client.executeStatement(req);
          printResp(resp);
        }
      }
      // TESTS END

      disconnect();
      transport.close();
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  private static void connect(String username, String password) {
    ConnectReq req = new ConnectReq();
    req.setUsername(username);
    req.setPassword(password);
    try {
      ConnectResp resp = client.connect(req);
      if (resp.status.code == Global.SUCCESS_CODE) {
        sessionID = resp.getSessionId();
        println(resp.status.getMsg());
      } else if (resp.status.code == Global.FAILURE_CODE) {
        sessionID = -1;
        println(resp.status.getMsg());
      }
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  private static void disconnect() {
    if (sessionID < 0) {
      println("you're not connected. plz connect first.");
      return;
    }
    DisconnectReq req = new DisconnectReq();
    req.setSessionId(sessionID);
    try {
      DisconnectResp resp = client.disconnect(req);
      if (resp.status.code == Global.SUCCESS_CODE) {
        sessionID = -1;
        println(resp.status.getMsg());
      } else if (resp.status.code == Global.FAILURE_CODE) {
        println(resp.status.getMsg());
      }
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  private static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  private static void println() {
    SCREEN_PRINTER.println();
  }

  private static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }

  private static void printResp(ExecuteStatementResp resp) {
    if (resp.status.code == Global.SUCCESS_CODE) {
      if (resp.hasResult) {
        // Calculate width of each column
        int column_size = resp.columnsList.size();
        int[] column_width = new int[column_size];
        for (int i = 0; i < column_size; ++i) {
          column_width[i] = resp.columnsList.get(i).length();
          for (List<String> row : resp.rowList) {
            int width = row.get(i).length();
            if (width > column_width[i]) column_width[i] = width;
          }
        }

        StringBuilder separator = new StringBuilder();
        for (int i = 0; i < column_size; ++i) {
          separator
              .append("+")
              .append(new String(new char[column_width[i] + 2]).replace("\0", "-"));
          if (i == column_size - 1) separator.append("+");
        }
        println(separator.toString());

        StringBuilder column_str = new StringBuilder();
        for (int i = 0; i < column_size; ++i) {
          column_str.append(
              String.format(String.format("| %%-%ds ", column_width[i]), resp.columnsList.get(i)));
          if (i == column_size - 1) column_str.append("|");
        }
        println(column_str.toString());

        println(separator.toString());

        for (List<String> row : resp.rowList) {
          StringBuilder row_str = new StringBuilder();
          for (int i = 0; i < column_size; ++i) {
            row_str.append(String.format(String.format("| %%-%ds ", column_width[i]), row.get(i)));
            if (i == column_size - 1) row_str.append("|");
          }
          println(row_str.toString());
        }

        println(separator.toString());
      } else {
        println(resp.status.getMsg());
      }
    } else if (resp.status.code == Global.FAILURE_CODE) {
      println(resp.status.getMsg());
    }
  }
}
