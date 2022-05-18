package com.linkedin;

import com.linkedin.pegasus.gradle.tasks.GenerateDataTemplateTask;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;


public class MCECreaterPlugin implements Plugin<Project> {

  private static final String GENERATE_SCHEMA_MXE_TASK_NAME = "GeneratePegasusSchemaMCE";

  @Override
  public void apply(Project project) {
    System.out.println("12344444");
    Set<SourceSet> sourceSets = getSourceSets(project);
    sourceSets.stream().forEach(sourceSet -> {
      Task generateDataTemplateTask = project.getTasks().findByName(sourceSet.getTaskName("generate", "DataTemplate"));
      if (!(generateDataTemplateTask instanceof GenerateDataTemplateTask)) {
        return;
      }
      final GenerateDataTemplateTask generateDataTemplatesTask = (GenerateDataTemplateTask) generateDataTemplateTask;

//      MCECreatorTask mceCreatorTask =
      project.getTasks()
          .create(sourceSet.getTaskName(sourceSet.getName(), GENERATE_SCHEMA_MXE_TASK_NAME), MCECreatorTask.class,
              task -> {
                task.setDescription(
                    String.format("generates MXE objects for pegasus schemas in %s into a file", sourceSet.getName()));
                task.setResolverPath(generateDataTemplatesTask.getResolverPath());
                task.setInputDir(generateDataTemplatesTask.getInputDir());
                task.dependsOn(
                    generateDataTemplatesTask); //depending modules need to publish their data-template.jars, before this task can be invoked
              });

//      project.getArtifacts().add(Dependency.DEFAULT_CONFIGURATION, mceCreatorTask);
    });
  }

  private Set<SourceSet> getSourceSets(Project project) {
    if (!project.getProperties().containsKey("sourceSets")) {
      return Collections.EMPTY_SET;
    }

    return ((SourceSetContainer) project.getProperties().get("sourceSets")).stream()
        .filter(sourceSet -> !sourceSet.getName()
            .toLowerCase()
            .contains("generated"))// pegasus creates few generated source sets. We dont want them
        .filter(sourceSet -> !sourceSet.getName()
            .equals("test")) // we don't to want to capture metadata for pegasus schemas modelled in test source set.
        .collect(Collectors.toSet());
  }
}
