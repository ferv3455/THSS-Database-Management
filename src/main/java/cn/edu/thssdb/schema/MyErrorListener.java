package cn.edu.thssdb.schema;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;

/** 描述：自定义的错误监听器，提供详细的错误信息，包括错误的位置 */
public class MyErrorListener extends BaseErrorListener {
  public static final MyErrorListener INSTANCE = new MyErrorListener();

  @Override
  public void syntaxError(
      Recognizer<?, ?> recognizer,
      Object offendingSymbol,
      int line,
      int charPositionInLine,
      String errorMessage,
      RecognitionException exception) {
    throw new ParseCancellationException(
        "line " + line + ":" + charPositionInLine + " " + errorMessage);
  }
}
