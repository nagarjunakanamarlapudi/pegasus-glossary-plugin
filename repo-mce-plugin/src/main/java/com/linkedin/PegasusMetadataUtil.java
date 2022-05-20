package com.linkedin;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.schema.DataSchemaLocation;
import com.linkedin.data.schema.NamedDataSchema;
import com.linkedin.data.schema.SchemaToPdlEncoder;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.aspect.DatasetAspect;
import com.linkedin.metadata.aspect.DatasetAspectArray;
import com.linkedin.metadata.snapshot.DatasetSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeEvent;
import com.linkedin.pegasus.generator.DataSchemaParser;
import com.linkedin.schema.OtherSchema;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.schemaannotation.GlossaryStructure;
import glossary.PegasusGlossaryExtractionUtils;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.gradle.api.file.FileCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PegasusMetadataUtil {
  private static final Logger LOG = LoggerFactory.getLogger(PegasusMetadataUtil.class);

  private PegasusMetadataUtil() {
  }

  @Nonnull
  public static List<SchemaInfo> parseSchemas(@Nonnull final File modelsLocation,
      @Nonnull final FileCollection resolverPath) throws IOException {
    final DataSchemaParser dataSchemaParser = new DataSchemaParser(resolverPath.getAsPath());
    final DataSchemaParser.ParseResult parsedSources =
        dataSchemaParser.parseSources(new String[]{modelsLocation.getAbsolutePath()});

    final Map<DataSchema, DataSchemaLocation> schemaLocations = parsedSources.getSchemaAndLocations();

    final List<SchemaInfo> schemaInfos = schemaLocations.entrySet()
        .stream()
        .filter(
            entry -> entry.getKey() instanceof NamedDataSchema)// only the named schemas will be onboarded to Data Hub
        .filter(entry -> !entry.getValue()
            .getSourceFile()
            .getAbsolutePath()
            .contains(".jar"))// schemas defined in the current module
        .map(entry -> new SchemaInfo((NamedDataSchema) entry.getKey(), getPegasusSchemaType(entry.getValue())))
        .collect(Collectors.toList());

    System.out.println(schemaInfos);
    System.out.println(schemaInfos.size());
    return schemaInfos;
  }

  public static List<String> getMCEs(@Nonnull final File modelsLocation, @Nonnull final FileCollection resolverPath)
      throws IOException {
    return parseSchemas(modelsLocation, resolverPath).stream()
        .filter(schemaInfo -> schemaInfo._namedDataSchema.getProperties().containsKey("dataset"))
        .map(PegasusMetadataUtil::createSchemaMCE)
        .collect(Collectors.toList());
  }

  @Nonnull
  private static String createSchemaMCE(@Nonnull SchemaInfo schemaInfo) {
    System.out.println("class path of plugin ... " + System.getProperties().get("java.class.path"));
    System.out.println("properties ... " + schemaInfo._namedDataSchema.getProperties());
    DatasetUrn datasetUrn = null;
    try {
      datasetUrn = DatasetUrn.deserialize(schemaInfo._namedDataSchema.getProperties().get("dataset").toString());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    MetadataChangeEvent metadataChangeEvent = new MetadataChangeEvent();

    DatasetSnapshot datasetSnapshot = new DatasetSnapshot();
    DatasetAspect datasetAspect = new DatasetAspect();
    SchemaMetadata schemaMetadata = new SchemaMetadata();
    schemaMetadata.setDataset(datasetUrn);
    schemaMetadata.setHash("hash");
    SchemaMetadata.PlatformSchema platformSchema = new SchemaMetadata.PlatformSchema();
    platformSchema.setOtherSchema(new OtherSchema().setRawSchema(schemaInfo._namedDataSchema.toString()));
    schemaMetadata.setPlatformSchema(platformSchema);
    schemaMetadata.setSchemaName(schemaInfo._namedDataSchema.getName());
    schemaMetadata.setPlatform(datasetUrn.getPlatformEntity());
    schemaMetadata.setVersion(0L);
    List<SchemaField> schemaFields = new ArrayList<>();

    final Map<String, Optional<List<GlossaryStructure>>> extractedGlossaryStructure =
        PegasusGlossaryExtractionUtils.extractGlossary(schemaInfo._namedDataSchema);

    for (Map.Entry<String, Optional<List<GlossaryStructure>>> entry : extractedGlossaryStructure.entrySet()) {
      if (!entry.getValue().isPresent()) {

        LOG.error(
            "error in checking presence of glossary annotations on pdl {} . Please reach out to ask_datahub@linkedin.com",
            schemaInfo._namedDataSchema.getFullName());
        continue;
      }
      final SchemaField schemaField = convertToSchemaField(entry.getKey(), entry.getValue());
      schemaFields.add(schemaField);
//      for (final GlossaryStructure glossaryStructure : entry.getValue().get()) {
//
//      }
    }

    schemaMetadata.setFields(new SchemaFieldArray(schemaFields));
    datasetAspect.setSchemaMetadata(schemaMetadata);

    datasetSnapshot.setAspects(new DatasetAspectArray(datasetAspect));
    datasetSnapshot.setUrn(datasetUrn);
    Snapshot snapshot = new Snapshot();
    snapshot.setDatasetSnapshot(datasetSnapshot);
    metadataChangeEvent.setProposedSnapshot(snapshot);

    String mce = null;
    try {
      mce = avroToJson(EventUtils.pegasusToAvroMCE(metadataChangeEvent));
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println(mce);

    return mce;
  }

  public static String avroToJson(GenericRecord record) throws IOException {
    DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(record.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
    datumWriter.write(record, encoder);
    encoder.flush();
    out.close();
    return out.toString("UTF-8");
  }

  private static SchemaField convertToSchemaField(String fieldPath,
      Optional<List<GlossaryStructure>> glossaryStructures) {
    SchemaField schemaField = new SchemaField();
    schemaField.setFieldPath(fieldPath);
    schemaField.setNullable(false);
    SchemaFieldDataType schemaFieldDataType = new SchemaFieldDataType();
    SchemaFieldDataType.Type type = new SchemaFieldDataType.Type();
    type.setStringType(new StringType());
    schemaFieldDataType.setType(type);
    schemaField.setNativeDataType("string");
    schemaField.setType(schemaFieldDataType);
    GlossaryTermAssociationArray glossaryTermAssociationArray = new GlossaryTermAssociationArray();

    glossaryStructures.ifPresent(structures -> structures.stream().forEach(glossaryStructure -> {
      GlossaryTermAssociation glossaryTermAssociation = new GlossaryTermAssociation();
      glossaryTermAssociation.setUrn(new GlossaryTermUrn(glossaryStructure.getGlossaryTerm()));
      glossaryTermAssociationArray.add(glossaryTermAssociation);
    }));

    GlossaryTerms glossaryTerms = new GlossaryTerms();
    glossaryTerms.setTerms(glossaryTermAssociationArray);
    glossaryTerms.setAuditStamp(new AuditStamp().setActor(new CorpuserUrn("naga")).setTime(1L));

    schemaField.setGlossaryTerms(glossaryTerms);

    return schemaField;
  }

  private static boolean areGlossaryAnnotationsValid(@Nonnull final NamedDataSchema namedDataSchema) {
    return getMissingGlossaryFields(namedDataSchema).isEmpty();
  }

  @Nonnull
  private static Set<String> getMissingGlossaryFields(@Nonnull final NamedDataSchema namedDataSchema) {
    return PegasusGlossaryExtractionUtils.extractGlossary(namedDataSchema)
        .entrySet()
        .stream()
        .filter(e -> !e.getValue().isPresent())
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  @Nonnull
  public static String getRawPDLSchema(@Nonnull final NamedDataSchema namedDataSchema) throws IOException {
    final StringWriter writer = new StringWriter();
    final SchemaToPdlEncoder encoder = new SchemaToPdlEncoder(writer);
    encoder.encode(namedDataSchema);
    return writer.toString();
  }

  @Nonnull
  private static PegasusModelType getPegasusSchemaType(@Nonnull final DataSchemaLocation value) {
    if (value.getSourceFile().getName().endsWith(".pdsc")) {
      return PegasusModelType.PDSC;
    } else if (value.getSourceFile().getName().endsWith(".pdl")) {
      return PegasusModelType.PDL;
    }
    return PegasusModelType.UNKNOWN;
  }

  static class SchemaInfo {
    final NamedDataSchema _namedDataSchema;
    final PegasusModelType _pegasusSchemaType;

    SchemaInfo(NamedDataSchema namedDataSchema, PegasusModelType pegasusSchemaType) {
      _namedDataSchema = namedDataSchema;
      _pegasusSchemaType = pegasusSchemaType;
    }

    @Override
    public String toString() {
      return "SchemaInfo{" + "_namedDataSchema=" + _namedDataSchema + ", _pegasusSchemaType=" + _pegasusSchemaType
          + '}';
    }
  }

  enum PegasusModelType {
    PDSC, PDL, UNKNOWN
  }
}
