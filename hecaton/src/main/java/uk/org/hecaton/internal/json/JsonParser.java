/*******************************************************************************
 * Copyright (c) 2013, 2016 EclipseSource.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package uk.org.hecaton.internal.json;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayDeque;
import java.util.Deque;


/**
 * A streaming parser for JSON text. The parser reports all events to a given handler.
 */
public class JsonParser {

  private static final int MAX_NESTING_LEVEL = 1000;
  private static final int MIN_BUFFER_SIZE = 10;
  private static final int DEFAULT_BUFFER_SIZE = 1024;

  private final JsonHandler<Object, Object> handler;
  private Reader reader;
  private char[] buffer;
  private int bufferOffset;
  private int index;
  private int fill;
  private int line;
  private int lineOffset;
  private int current;
  private StringBuilder captureBuffer;
  private int captureStart;
  private int nestingLevel;

  /*
   * |                      bufferOffset
   *                        v
   * [a|b|c|d|e|f|g|h|i|j|k|l|m|n|o|p|q|r|s|t]        < input
   *                       [l|m|n|o|p|q|r|s|t|?|?]    < buffer
   *                          ^               ^
   *                       |  index           fill
   */

  /**
   * Creates a new JsonParser with the given handler. The parser will report all parser events to
   * this handler.
   *
   * @param handler
   *          the handler to process parser events
   */
  @SuppressWarnings("unchecked")
  public JsonParser(JsonHandler<?, ?> handler) {
    if (handler == null) {
      throw new NullPointerException("handler is null");
    }
    this.handler = (JsonHandler<Object, Object>)handler;
    handler.parser = this;
  }

  /**
   * Parses the given input string. The input must contain a valid JSON value, optionally padded
   * with whitespace.
   *
   * @param string
   *          the input string, must be valid JSON
   * @throws ParseException
   *           if the input is not valid JSON
   */
  public void parse(String string) {
    if (string == null) {
      throw new NullPointerException("string is null");
    }
    int bufferSize = Math.max(MIN_BUFFER_SIZE, Math.min(DEFAULT_BUFFER_SIZE, string.length()));
    try {
      parse(new StringReader(string), bufferSize);
    } catch (IOException exception) {
      // StringReader does not throw IOException
      throw new RuntimeException(exception);
    }
  }

  /**
   * Reads the entire input from the given reader and parses it as JSON. The input must contain a
   * valid JSON value, optionally padded with whitespace.
   * <p>
   * Characters are read in chunks into a default-sized input buffer. Hence, wrapping a reader in an
   * additional <code>BufferedReader</code> likely won't improve reading performance.
   * </p>
   *
   * @param reader
   *          the reader to read the input from
   * @throws IOException
   *           if an I/O error occurs in the reader
   * @throws ParseException
   *           if the input is not valid JSON
   */
  public void parse(Reader reader) throws IOException {
    parse(reader, DEFAULT_BUFFER_SIZE);
  }

  /**
   * Reads the entire input from the given reader and parses it as JSON. The input must contain a
   * valid JSON value, optionally padded with whitespace.
   * <p>
   * Characters are read in chunks into an input buffer of the given size. Hence, wrapping a reader
   * in an additional <code>BufferedReader</code> likely won't improve reading performance.
   * </p>
   *
   * @param reader
   *          the reader to read the input from
   * @param buffersize
   *          the size of the input buffer in chars
   * @throws IOException
   *           if an I/O error occurs in the reader
   * @throws ParseException
   *           if the input is not valid JSON
   */
  public void parse(Reader reader, int buffersize) throws IOException {
    if (reader == null) {
      throw new NullPointerException("reader is null");
    }
    if (buffersize <= 0) {
      throw new IllegalArgumentException("buffersize is zero or negative");
    }
    this.reader = reader;
    buffer = new char[buffersize];
    bufferOffset = 0;
    index = 0;
    fill = 0;
    line = 1;
    lineOffset = 0;
    current = 0;
    captureStart = -1;
    read();
    skipWhiteSpace();
    readValue();
    skipWhiteSpace();
    if (!isEndOfText()) {
      throw error("Unexpected character");
    }
  }

  /**
   * Reads one value. Arrays and objects are read with an explicit stack rather than by
   * recursion, so deep nesting cannot overflow the thread's stack before
   * {@link #MAX_NESTING_LEVEL} is reached; the handler sees the same calls in the same order.
   */
  private void readValue() throws IOException {
    Deque<Container> stack = new ArrayDeque<>();
    while (true) {
      if (current == '[') {
        Object array = handler.startArray();
        read();
        if (++nestingLevel > MAX_NESTING_LEVEL) {
          throw error("Nesting too deep");
        }
        skipWhiteSpace();
        if (readChar(']')) {
          nestingLevel--;
          handler.endArray(array);
        } else {
          stack.push(new Container(array, true));
          skipWhiteSpace();
          handler.startArrayValue(array);
          continue;
        }
      } else if (current == '{') {
        Object object = handler.startObject();
        read();
        if (++nestingLevel > MAX_NESTING_LEVEL) {
          throw error("Nesting too deep");
        }
        skipWhiteSpace();
        if (readChar('}')) {
          nestingLevel--;
          handler.endObject(object);
        } else {
          Container container = new Container(object, false);
          stack.push(container);
          readMemberName(container);
          continue;
        }
      } else {
        readScalar();
      }
      // a value is complete: close the containers it completes, up to the next value to read
      if (!finishValue(stack)) {
        return;
      }
    }
  }

  /**
   * Called when a value inside the innermost container is complete. Returns {@code true} when
   * another value of some container follows, {@code false} when the outermost value is complete.
   */
  private boolean finishValue(Deque<Container> stack) throws IOException {
    while (!stack.isEmpty()) {
      Container container = stack.peek();
      if (container.isArray) {
        handler.endArrayValue(container.value);
        skipWhiteSpace();
        if (readChar(',')) {
          skipWhiteSpace();
          handler.startArrayValue(container.value);
          return true;
        }
        if (!readChar(']')) {
          throw expected("',' or ']'");
        }
        stack.pop();
        nestingLevel--;
        handler.endArray(container.value);
      } else {
        handler.endObjectValue(container.value, container.name);
        skipWhiteSpace();
        if (readChar(',')) {
          readMemberName(container);
          return true;
        }
        if (!readChar('}')) {
          throw expected("',' or '}'");
        }
        stack.pop();
        nestingLevel--;
        handler.endObject(container.value);
      }
    }
    return false;
  }

  private void readMemberName(Container object) throws IOException {
    skipWhiteSpace();
    handler.startObjectName(object.value);
    String name = readName();
    handler.endObjectName(object.value, name);
    skipWhiteSpace();
    if (!readChar(':')) {
      throw expected("':'");
    }
    skipWhiteSpace();
    object.name = name;
    handler.startObjectValue(object.value, name);
  }

  private void readScalar() throws IOException {
    switch (current) {
      case 'n':
        readNull();
        break;
      case 't':
        readTrue();
        break;
      case 'f':
        readFalse();
        break;
      case '"':
        readString();
        break;
      case '-':
      case '0':
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        readNumber();
        break;
      default:
        throw expected("value");
    }
  }

  /** An array or object being read, with the name of the object member being read. */
  private static final class Container {
    final Object value;
    final boolean isArray;
    String name;

    Container(Object value, boolean isArray) {
      this.value = value;
      this.isArray = isArray;
    }
  }

  private String readName() throws IOException {
    if (current != '"') {
      throw expected("name");
    }
    return readStringInternal();
  }

  private void readNull() throws IOException {
    handler.startNull();
    read();
    readRequiredChar('u');
    readRequiredChar('l');
    readRequiredChar('l');
    handler.endNull();
  }

  private void readTrue() throws IOException {
    handler.startBoolean();
    read();
    readRequiredChar('r');
    readRequiredChar('u');
    readRequiredChar('e');
    handler.endBoolean(true);
  }

  private void readFalse() throws IOException {
    handler.startBoolean();
    read();
    readRequiredChar('a');
    readRequiredChar('l');
    readRequiredChar('s');
    readRequiredChar('e');
    handler.endBoolean(false);
  }

  private void readRequiredChar(char ch) throws IOException {
    if (!readChar(ch)) {
      throw expected("'" + ch + "'");
    }
  }

  private void readString() throws IOException {
    handler.startString();
    handler.endString(readStringInternal());
  }

  private String readStringInternal() throws IOException {
    read();
    startCapture();
    while (current != '"') {
      if (current == '\\') {
        pauseCapture();
        readEscape();
        startCapture();
      } else if (current < 0x20) {
        throw expected("valid string character");
      } else {
        read();
      }
    }
    String string = endCapture();
    read();
    return string;
  }

  private void readEscape() throws IOException {
    read();
    switch (current) {
      case '"', '/', '\\':
        captureBuffer.append((char)current);
        break;
      case 'b':
        captureBuffer.append('\b');
        break;
      case 'f':
        captureBuffer.append('\f');
        break;
      case 'n':
        captureBuffer.append('\n');
        break;
      case 'r':
        captureBuffer.append('\r');
        break;
      case 't':
        captureBuffer.append('\t');
        break;
      case 'u':
        char[] hexChars = new char[4];
        for (int i = 0; i < 4; i++) {
          read();
          if (!isHexDigit()) {
            throw expected("hexadecimal digit");
          }
          hexChars[i] = (char)current;
        }
        captureBuffer.append((char)Integer.parseInt(new String(hexChars), 16));
        break;
      default:
        throw expected("valid escape sequence");
    }
    read();
  }

  private void readNumber() throws IOException {
    handler.startNumber();
    startCapture();
    readChar('-');
    int firstDigit = current;
    if (!readDigit()) {
      throw expected("digit");
    }
    if (firstDigit != '0') {
      while (readDigit()) {
      }
    }
    readFraction();
    readExponent();
    handler.endNumber(endCapture());
  }

  private boolean readFraction() throws IOException {
    if (!readChar('.')) {
      return false;
    }
    if (!readDigit()) {
      throw expected("digit");
    }
    while (readDigit()) {
    }
    return true;
  }

  private boolean readExponent() throws IOException {
    if (!readChar('e') && !readChar('E')) {
      return false;
    }
    if (!readChar('+')) {
      readChar('-');
    }
    if (!readDigit()) {
      throw expected("digit");
    }
    while (readDigit()) {
    }
    return true;
  }

  private boolean readChar(char ch) throws IOException {
    if (current != ch) {
      return false;
    }
    read();
    return true;
  }

  private boolean readDigit() throws IOException {
    if (!isDigit()) {
      return false;
    }
    read();
    return true;
  }

  private void skipWhiteSpace() throws IOException {
    while (isWhiteSpace()) {
      read();
    }
  }

  private void read() throws IOException {
    if (index == fill) {
      if (captureStart != -1) {
        captureBuffer.append(buffer, captureStart, fill - captureStart);
        captureStart = 0;
      }
      bufferOffset += fill;
      fill = reader.read(buffer, 0, buffer.length);
      index = 0;
      if (fill == -1) {
        current = -1;
        index++;
        return;
      }
    }
    if (current == '\n') {
      line++;
      lineOffset = bufferOffset + index;
    }
    current = buffer[index++];
  }

  private void startCapture() {
    if (captureBuffer == null) {
      captureBuffer = new StringBuilder();
    }
    captureStart = index - 1;
  }

  private void pauseCapture() {
    int end = current == -1 ? index : index - 1;
    captureBuffer.append(buffer, captureStart, end - captureStart);
    captureStart = -1;
  }

  private String endCapture() {
    int start = captureStart;
    int end = index - 1;
    captureStart = -1;
    if (captureBuffer.length() > 0) {
      captureBuffer.append(buffer, start, end - start);
      String captured = captureBuffer.toString();
      captureBuffer.setLength(0);
      return captured;
    }
    return new String(buffer, start, end - start);
  }

  Location getLocation() {
    int offset = bufferOffset + index - 1;
    int column = offset - lineOffset + 1;
    return new Location(offset, line, column);
  }

  private ParseException expected(String expected) {
    if (isEndOfText()) {
      return error("Unexpected end of input");
    }
    return error("Expected " + expected);
  }

  private ParseException error(String message) {
    return new ParseException(message, getLocation());
  }

  private boolean isWhiteSpace() {
    return current == ' ' || current == '\t' || current == '\n' || current == '\r';
  }

  private boolean isDigit() {
    return current >= '0' && current <= '9';
  }

  private boolean isHexDigit() {
    return current >= '0' && current <= '9'
        || current >= 'a' && current <= 'f'
        || current >= 'A' && current <= 'F';
  }

  private boolean isEndOfText() {
    return current == -1;
  }

}
