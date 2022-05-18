package glossary;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.data.schema.annotation.PegasusSchemaAnnotationHandlerImpl;
import com.linkedin.data.schema.annotation.ResolvedPropertiesReaderVisitor;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import com.linkedin.data.schema.annotation.SchemaAnnotationProcessor;
import com.linkedin.schemaannotation.GlossaryStructure;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extract glossary for pegasus models
 */
public class PegasusGlossaryExtractionUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PegasusGlossaryExtractionUtils.class);
  private static final PegasusSchemaAnnotationHandlerImpl CUSTOM_ANNOTATION_HANDLER =
      new PegasusSchemaAnnotationHandlerImpl(PegasusGlossaryValidationUtils.GLOSSARY_ANNOTATION_NAMESPACE);
  private static final SchemaAnnotationProcessor.AnnotationProcessOption ANNOTATION_PROCESS_OPTION =
      new SchemaAnnotationProcessor.AnnotationProcessOption();

  private PegasusGlossaryExtractionUtils() {

  }

  /**
   * This doesn't fail the process if the glossary is not valid.
   * This is not called during build of the pegasus models for glossary.
   * Any other dependents mps which are willing to extract the glossary for a pegasus schema can use this utility.
   *
   * Validity of the glossary is derived using {@link PegasusGlossaryValidationUtils}
   * - Returns glossary for the fields where glossary annotations are valid.
   * - Returns an empty value for the fields where glossary is not present or semantically invalid.
   * @param dataSchema {@link DataSchema} object for which glossary is being extracted
   * @return map of pegasus path spec and glossary specified in the pegasus models
   */
  @Nonnull
  public static Map<String, Optional<List<GlossaryStructure>>> extractGlossary(@Nonnull DataSchema dataSchema) {

    final Map<String, Optional<List<GlossaryStructure>>> glossaryAnnotations = new HashMap<>();

    final Map<PathSpec, Map<String, Object>> pathSpecToResolvedPropertiesMap =
        getPathSpecToResolvedPropertiesMap(dataSchema);

    for (Map.Entry<PathSpec, Map<String, Object>> entry : pathSpecToResolvedPropertiesMap.entrySet()) {
      final String pathSpec = entry.getKey().toString();
      final Map<String, Object> resolvedAnnotationProperties = entry.getValue();
      final SchemaAnnotationHandler.AnnotationValidationResult validationResult =
          PegasusGlossaryValidationUtils.validate(resolvedAnnotationProperties);
      if (validationResult.isValid()) {
        glossaryAnnotations.put(pathSpec, getglossaryStructures(
            resolvedAnnotationProperties.get(PegasusGlossaryValidationUtils.GLOSSARY_ANNOTATION_NAMESPACE)));
      } else {
        LOGGER.debug("glossary annotations are not valid for {} ", pathSpec);
        glossaryAnnotations.put(pathSpec, Optional.empty());
      }
    }
    return glossaryAnnotations;
  }

  @Nonnull
  private static Optional<List<GlossaryStructure>> getglossaryStructures(@Nonnull Object resolvedglossary) {
    List<GlossaryStructure> glossaryStructures = null;
    if (resolvedglossary instanceof String && resolvedglossary.equals(
        PegasusGlossaryValidationUtils.NO_GLOSSARY_STRING)) {
      glossaryStructures = Collections.singletonList(new GlossaryStructure().setGlossaryTerm(""));
    } else if (resolvedglossary instanceof DataMap) {
      glossaryStructures = Collections.singletonList(new GlossaryStructure((DataMap) resolvedglossary));
    } else if (resolvedglossary instanceof DataList) {
      glossaryStructures = ((DataList) resolvedglossary).stream()
          .map(e -> new GlossaryStructure((DataMap) e))
          .collect(Collectors.toList());
    }

    return Optional.ofNullable(glossaryStructures);
  }

  @Nonnull
  private static Map<PathSpec, Map<String, Object>> getPathSpecToResolvedPropertiesMap(@Nonnull DataSchema dataSchema) {
    final SchemaAnnotationProcessor.SchemaAnnotationProcessResult result =
        SchemaAnnotationProcessor.process(Collections.singletonList(CUSTOM_ANNOTATION_HANDLER), dataSchema,
            ANNOTATION_PROCESS_OPTION);
    final ResolvedPropertiesReaderVisitor resolvedPropertiesReaderVisitor = new ResolvedPropertiesReaderVisitor();
    final DataSchemaRichContextTraverser traverser =
        new DataSchemaRichContextTraverser(resolvedPropertiesReaderVisitor);
    traverser.traverse(result.getResultSchema());
    final Map<PathSpec, Map<String, Object>> resolvedPropertiesMap =
        resolvedPropertiesReaderVisitor.getLeafFieldsToResolvedPropertiesMap();
    return resolvedPropertiesMap.entrySet()
        .stream()
        .filter(entry -> !entry.getKey().toString().endsWith("/null")) //don't report glossary for /null
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}