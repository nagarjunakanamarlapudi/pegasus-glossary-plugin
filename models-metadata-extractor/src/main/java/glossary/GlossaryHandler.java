package glossary;

import com.linkedin.data.schema.NullDataSchema;
import com.linkedin.data.schema.annotation.DataSchemaRichContextTraverser;
import com.linkedin.data.schema.annotation.PegasusSchemaAnnotationHandlerImpl;
import com.linkedin.data.schema.annotation.RestLiSchemaAnnotationHandler;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of annotation handler {@link PegasusSchemaAnnotationHandlerImpl} for glossary metadata.
 */
@RestLiSchemaAnnotationHandler
public class GlossaryHandler extends PegasusSchemaAnnotationHandlerImpl {
  private static final Logger _log = LoggerFactory.getLogger(GlossaryHandler.class);

  public GlossaryHandler() {
    super(PegasusGlossaryValidationUtils.GLOSSARY_ANNOTATION_NAMESPACE);
  }

  @Override
  public AnnotationValidationResult validate(Map<String, Object> resolvedProperties, ValidationMetaData metaData) {
    final AnnotationValidationResult validationResult = new AnnotationValidationResult();
    if (DataSchemaRichContextTraverser.isLeafSchema(metaData.getDataSchema())
        && !(metaData.getDataSchema() instanceof NullDataSchema)) {
      try {
        return PegasusGlossaryValidationUtils.validate(resolvedProperties);
      } catch (Exception e) {
        validationResult.setValid(false);
        validationResult.addMessage(Arrays.asList(metaData.getPathToSchema().toArray(new String[0])), e.getMessage());
      }
    }
    return validationResult;
  }
}
