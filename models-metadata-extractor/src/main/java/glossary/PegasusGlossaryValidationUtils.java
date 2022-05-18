package glossary;

import com.linkedin.data.DataMap;
import com.linkedin.data.message.Message;
import com.linkedin.data.schema.ArrayDataSchema;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.annotation.SchemaAnnotationHandler;
import com.linkedin.data.schema.validation.CoercionMode;
import com.linkedin.data.schema.validation.RequiredMode;
import com.linkedin.data.schema.validation.UnrecognizedFieldMode;
import com.linkedin.data.schema.validation.ValidateDataAgainstSchema;
import com.linkedin.data.schema.validation.ValidationOptions;
import com.linkedin.data.schema.validation.ValidationResult;
import com.linkedin.schemaannotation.GlossaryStructure;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * Validates glossary on pegasus models
 */
public class PegasusGlossaryValidationUtils {

  static final String NO_GLOSSARY_STRING = "NONE";
//  DatasetAspect

  static final GlossaryStructure GLOSSARY_STRUCTURE = new GlossaryStructure();
  static final String GLOSSARY_ANNOTATION_NAMESPACE = "glossary";

  // Check both (1) missing data for required field
  // (2) excessive data that not on the schema
  public static final ValidationOptions VALIDATION_OPTIONS =
      new ValidationOptions(RequiredMode.MUST_BE_PRESENT, CoercionMode.STRING_TO_PRIMITIVE,
          UnrecognizedFieldMode.DISALLOW);

  private PegasusGlossaryValidationUtils() {

  }

  public static SchemaAnnotationHandler.AnnotationValidationResult validate(
      @Nonnull Map<String, Object> resolvedProperties) {
    final SchemaAnnotationHandler.AnnotationValidationResult validationResult =
        new SchemaAnnotationHandler.AnnotationValidationResult();

    final Object dataElement = resolvedProperties.get(GLOSSARY_ANNOTATION_NAMESPACE);
    if (dataElement != null) {
      // glossary could be "NONE"
      if ((dataElement instanceof String) && ((String) dataElement).equals(NO_GLOSSARY_STRING)) {
        return validationResult;
      }

      final DataSchema glossarySchema = GLOSSARY_STRUCTURE.schema();
      // Glossary is supposed to be annotated in the form of an array of "glossaryStructure"s
      // but if there is only one glossary element, it does not have to be wrapped in an array
      // and is regarded as a syntactic sugar.
      ValidationResult result = null;
      if (dataElement instanceof DataMap) {
        result = ValidateDataAgainstSchema.validate(dataElement, glossarySchema, VALIDATION_OPTIONS);
      } else {
        ArrayDataSchema glossaryArrayDataSchema = new ArrayDataSchema(glossarySchema);
        result = ValidateDataAgainstSchema.validate(dataElement, glossaryArrayDataSchema, VALIDATION_OPTIONS);
      }
      validationResult.setValid(result.isValid());
      if (!result.isValid()) {
        validationResult.addMessages(result.getMessages());
      }
      return validationResult;
    } else {
      // Error case: glossary must be defined in every leaf DataSchema.
      validationResult.setValid(false);
      validationResult.addMessage(new Message(new Object[]{}, "Glossary annotations are not found!"));
      return validationResult;
    }
  }
}