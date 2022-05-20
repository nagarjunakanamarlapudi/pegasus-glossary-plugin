package com.linkedin;

import java.io.File;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;


public class MCECreatorTask extends DefaultTask {
  // models location
  private File _inputDir;

  // resolver path to parse the models
  private FileCollection _resolverPath;

  private static final String MXE_OUTPUT_DIR = "GeneratedMXE";

  @TaskAction
  public void performTask() throws Exception {
    _resolverPath = _resolverPath.plus(getProject().files(_inputDir));
    buildMCEEvents(_inputDir, _resolverPath);
  }

  @Nonnull
  public static List<String> buildMCEEvents(@Nonnull final File modelsLocation,
      @Nonnull final FileCollection resolverPath) throws IOException {
    List<String> output = PegasusMetadataUtil.getMCEs(modelsLocation, resolverPath);
    writeToJson(output, "/Users/nkanamar/Desktop/mces.json");
    return output;
  }

  private static void writeToJson(List<String> output, String fileLocation) {

    StringBuffer stringBuffer = new StringBuffer().append("[").append("\n");
    for (int i = 0; i < output.size(); i++) {
      if (i == 0) {
        stringBuffer.append(output.get(i)).append("\n");
      } else {
        stringBuffer.append(",").append(output.get(i)).append("\n");
      }
    }
    stringBuffer.append("\n").append("]");

    try {
      FileUtils.writeStringToFile(new File(fileLocation), stringBuffer.toString());
    } catch (IOException ex) {
      // Handle exception
    }
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
