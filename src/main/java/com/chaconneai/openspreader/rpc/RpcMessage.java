package com.chaconneai.openspreader.rpc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A message of the RPC protocol.
 *
 * <p>Same approach as the lock, the semaphore and task dispatch: spreader's business channel
 * is one-way, so the message carries a {@code requestId} of its own to match a response to
 * its request.
 *
 * <h2>Arguments are encoded one at a time, not as one array</h2>
 * Because deserialising <b>needs to know the target type</b>, and the types are per argument,
 * written on the method signature. Encoding the whole {@code Object[]} together would leave
 * the decoding side with a heap of things and no way to restore each to its own type.
 *
 * <p>The message <b>does not carry argument type names</b>: having found the method by
 * {@code beanName + methodName + argument count}, the server reads the types off the
 * signature. One fewer thing to transmit, and one fewer place where the two sides can
 * disagree.
 *
 * <h2>Frame layout, big-endian</h2>
 * <pre>
 * magic(4B) | version(1B) | type(1B) | requestId(8B)
 *           | beanName(UTF) | className(UTF) | methodName(UTF)
 *           | argCount(4B) | [ argLen(4B) | argBytes ] * argCount
 *           | returnLen(4B) | returnBytes
 *           | success(1B) | errorType(UTF) | errorMessage(UTF)
 * </pre>
 *
 * <p>{@code errorType} is the fully-qualified class name of the peer's exception. Carrying
 * it is what gives the caller a chance to <b>restore the exception to its original
 * type</b>; where the class is absent from this process it degenerates to
 * {@link RpcException}, losing no information.
 *
 * @author Fred Feng
 * @version 1.0.0
 * @since 19/08/2026
 */
public record RpcMessage(
        RpcMessageType type,
        long requestId,
        String beanName,
        String className,
        String methodName,
        List<byte[]> args,
        byte[] returnValue,
        boolean success,
        String errorType,
        String errorMessage) {

    /** Magic number "SPRC", for SPreader Rpc. */
    public static final int MAGIC = 0x53505243;

    public static final byte VERSION = 1;

    private static final byte[] NO_BYTES = new byte[0];

    public RpcMessage {
        args = args == null ? List.of() : List.copyOf(args);
        returnValue = returnValue == null ? NO_BYTES : returnValue;
        errorType = errorType == null ? "" : errorType;
        errorMessage = errorMessage == null ? "" : errorMessage;
    }

    public static RpcMessage request(long requestId, String beanName, String className,
                                     String methodName, List<byte[]> args) {
        return new RpcMessage(RpcMessageType.REQUEST, requestId, beanName, className, methodName,
                args, NO_BYTES, false, "", "");
    }

    public static RpcMessage ok(long requestId, byte[] returnValue) {
        return new RpcMessage(RpcMessageType.RESPONSE, requestId, "", "", "",
                List.of(), returnValue, true, "", "");
    }

    public static RpcMessage fail(long requestId, String errorType, String errorMessage) {
        return new RpcMessage(RpcMessageType.RESPONSE, requestId, "", "", "",
                List.of(), NO_BYTES, false, errorType, errorMessage);
    }

    /**
     * Whether these bytes are an RPC protocol message.
     *
     * <p>Only RPC runs on this channel, but with nodes restarting and versions mixed, anything
     * can arrive -- so checking the magic number first is safer than parsing outright.
     */
    public static boolean matches(byte[] content) {
        if (content == null || content.length < 6) {
            return false;
        }
        int magic = ((content[0] & 0xFF) << 24) | ((content[1] & 0xFF) << 16)
                | ((content[2] & 0xFF) << 8) | (content[3] & 0xFF);
        return magic == MAGIC;
    }

    public byte[] encode() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bos)) {
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            out.writeByte(type.code());
            out.writeLong(requestId);
            out.writeUTF(beanName == null ? "" : beanName);
            out.writeUTF(className == null ? "" : className);
            out.writeUTF(methodName == null ? "" : methodName);

            out.writeInt(args.size());
            for (byte[] arg : args) {
                byte[] safe = arg == null ? NO_BYTES : arg;
                out.writeInt(safe.length);
                out.write(safe);
            }

            out.writeInt(returnValue.length);
            out.write(returnValue);

            out.writeBoolean(success);
            out.writeUTF(errorType);
            // An exception message can be long, carrying a stack summary, and writeUTF caps at
            // 65535 bytes and throws beyond that. Truncating beats failing to send the response
            // at all -- that would leave the caller with nothing but a timeout
            out.writeUTF(truncate(errorMessage));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** @return the decoded message, or null when it is not this protocol, the version is
     *          unrecognised, or the format is wrong */
    public static RpcMessage decode(byte[] content) {
        if (!matches(content)) {
            return null;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(content))) {
            in.readInt();
            if (in.readByte() != VERSION) {
                return null;
            }
            RpcMessageType type = RpcMessageType.fromCode(in.readByte());
            if (type == null) {
                return null;
            }
            long requestId = in.readLong();
            String beanName = in.readUTF();
            String className = in.readUTF();
            String methodName = in.readUTF();

            int argCount = in.readInt();
            if (argCount < 0 || argCount > 255) {
                // No method has hundreds of arguments, so a count like this means the frame
                // itself is corrupt
                return null;
            }
            List<byte[]> args = new ArrayList<>(argCount);
            for (int i = 0; i < argCount; i++) {
                int len = in.readInt();
                if (len < 0 || len > content.length) {
                    return null;
                }
                byte[] arg = new byte[len];
                in.readFully(arg);
                args.add(arg);
            }

            int returnLen = in.readInt();
            if (returnLen < 0 || returnLen > content.length) {
                return null;
            }
            byte[] returnValue = new byte[returnLen];
            in.readFully(returnValue);

            return new RpcMessage(type, requestId, beanName, className, methodName, args,
                    returnValue, in.readBoolean(), in.readUTF(), in.readUTF());
        } catch (IOException e) {
            return null;
        }
    }

    /** writeUTF caps at 65535 bytes, and one non-ASCII character can take 3 or 4 of them in
     *  UTF-8, so this leaves room for the worst case. */
    private static String truncate(String s) {
        int max = 20_000;
        return s.length() <= max ? s : s.substring(0, max) + "... (truncated)";
    }
}
