package com.linkedin;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class MCECreatorTask extends DefaultTask {
  // models location
  private File _inputDir;

  // resolver path to parse the models
  private FileCollection _resolverPath;

  private static final String MXE_OUTPUT_DIR = "GeneratedMXE";

  @TaskAction
  public void performTask() throws Exception {
    System.out.println("in mce creator task");
    _resolverPath = _resolverPath.plus(getProject().files(_inputDir));
    buildMCEEvents(_inputDir, _resolverPath);
  }

  @Nonnull
  public static List<String> buildMCEEvents(@Nonnull final File modelsLocation,
      @Nonnull final FileCollection resolverPath) throws IOException {
    System.out.println("models location .. " + modelsLocation);
    System.out.println("resolver path .. " + resolverPath);
    List<String> output = PegasusMetadataUtil.getMCEs(modelsLocation, resolverPath);
    System.out.println("output ... " + output);
    System.out.println("output ... " + output.size());

    writeToJson(output, "/Users/nkanamar/Desktop/mces.json");

    return output;
  }

  private static void writeToJson(List<String> output, String fileLocation) {
    //Creating a JSONObject object
    JSONObject jsonObject = new JSONObject();
    //Inserting key-value pairs into the json object
    JSONArray jsonArray = new JSONArray();
    output.stream().forEach(event -> jsonArray.add(event));
    try {
      FileWriter file = new FileWriter(fileLocation);
      file.write(jsonArray.toJSONString());
      file.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    System.out.println("JSON file created: " + jsonObject);
  }

  @OutputDirectory
  protected File getMxeDir() {
    final File buildDir = getProject().getBuildDir();
    return new File(buildDir.getPath() + File.separator + String.format("%s_%s", getName(), MXE_OUTPUT_DIR));
  }

  public void setInputDir(File inputDir) {
    _inputDir = inputDir;
  }

  public void setResolverPath(FileCollection resolverPath) {
    _resolverPath = resolverPath;
  }
}
