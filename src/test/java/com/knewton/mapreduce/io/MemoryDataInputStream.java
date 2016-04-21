package com.knewton.mapreduce.io;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Helper class implementation of {@link DataInputStream} that wraps a {@link ByteArrayInputStream}
 * and is {@link Seekable} for use in tests
 *
 * @author Giannis Neokleous
 *
 */
public class MemoryDataInputStream extends DataInputStream implements Seekable, PositionedReadable {

    private final byte[] buf;

    public MemoryDataInputStream(byte[] buf) {
        super(new ByteArrayInputStream(buf));
        this.buf = buf;
    }

    @Override
    public void seek(long pos) throws IOException {
        in.reset();
        in.skip(pos);
    }

    @Override
    public long getPos() throws IOException {
        return buf.length - in.available();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        seek(position);
        return in.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        seek(position);
        in.read(buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        seek(position);
        in.read(buffer);
    }

}
