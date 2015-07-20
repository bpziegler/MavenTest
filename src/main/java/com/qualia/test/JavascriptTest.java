package com.qualia.test;


import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;


public class JavascriptTest {

    public static String loadStreamToString(final InputStream is) {
        final char[] buffer = new char[8192];
        final StringBuilder out = new StringBuilder();
        try (Reader in = new InputStreamReader(is, "UTF-8")) {
            for (;;) {
                int rsz = in.read(buffer, 0, buffer.length);
                if (rsz < 0)
                    break;
                out.append(buffer, 0, rsz);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out.toString();
    }


    public static void main(String[] args) throws Exception {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("test.js");
        String scriptStr = loadStreamToString(is);

        ScriptEngineManager factory = new ScriptEngineManager();
        ScriptEngine engine = factory.getEngineByName("JavaScript");
        if (engine instanceof Compilable) {
            System.out.println("Compiling");
            Compilable compilable = (Compilable) engine;
            CompiledScript compiledScript = compilable.compile(scriptStr);
            compiledScript.eval();
        } else {
            System.out.println("Interpreting");
            engine.eval(scriptStr);
        }
    }
}
