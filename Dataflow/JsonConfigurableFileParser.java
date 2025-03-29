import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Paths;

// JSON processing
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

// Base interface for all record types
interface Record {
}

class DataRecord implements Record {
    HeaderRecord header;
    BaseRecord base;

    @Override
    public String toString() {
        return "DataRecord{" +
                "header=" + header +
                ", base=" + base +
                '}';
    }
}

class HeaderRecord implements Record {
    Map<String, String> fields = new HashMap<>();

    public void setField(String name, String value) {
        fields.put(name, value);
    }

    public String getField(String name) {
        return fields.get(name);
    }

    @Override
    public String toString() {
        return "HeaderRecord" + fields;
    }
}

class BaseRecord implements Record {
    Map<String, String> fields = new HashMap<>();
    Map<String, List<Record>> childRecords = new HashMap<>();

    public void setField(String name, String value) {
        fields.put(name, value);
    }

    public String getField(String name) {
        return fields.get(name);
    }

    public List<Record> getOrCreateList(String recordType) {
        return childRecords.computeIfAbsent(recordType, k -> new ArrayList<>());
    }

    @Override
    public String toString() {
        return "BaseRecord{" +
                "fields=" + fields +
                ", childRecords=" + childRecords +
                '}';
    }
}

class SegmentRecord implements Record {
    Map<String, String> fields = new HashMap<>();
    String type;

    public void setField(String name, String value) {
        fields.put(name, value);
    }

    public String getField(String name) {
        return fields.get(name);
    }

    @Override
    public String toString() {
        return type + "Record" + fields;
    }
}

// Configuration class to store segment layout information
class LayoutConfig {
    private Map<String, RecordLayout> layouts = new HashMap<>();

    public void addLayout(String type, RecordLayout layout) {
        layouts.put(type, layout);
    }

    public RecordLayout getLayout(String type) {
        return layouts.get(type);
    }

    public boolean hasLayout(String type) {
        return layouts.containsKey(type);
    }

    // Parse layout from JSON file
    public static LayoutConfig fromJsonFile(String jsonPath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readTree(new String(Files.readAllBytes(Paths.get(jsonPath))));
        
        LayoutConfig config = new LayoutConfig();
        
        // Process each record type defined in the JSON
        rootNode.fields().forEachRemaining(entry -> {
            String recordType = entry.getKey();
            JsonNode layoutNode = entry.getValue();
            
            RecordLayout layout = new RecordLayout();
            layout.setFixedLength(layoutNode.path("fixedLength").asBoolean(false));
            layout.setTotalLength(layoutNode.path("totalLength").asInt(0));
            
            // Process fields for this record type
            JsonNode fieldsNode = layoutNode.path("fields");
            fieldsNode.fields().forEachRemaining(fieldEntry -> {
                String fieldName = fieldEntry.getKey();
                JsonNode fieldNode = fieldEntry.getValue();
                
                FieldDefinition fieldDef = new FieldDefinition();
                fieldDef.setName(fieldName);
                fieldDef.setStartPos(fieldNode.path("startPos").asInt(0));
                fieldDef.setLength(fieldNode.path("length").asInt(0));
                
                layout.addField(fieldDef);
            });
            
            config.addLayout(recordType, layout);
        });
        
        return config;
    }
}

class RecordLayout {
    private boolean fixedLength;
    private int totalLength;
    private List<FieldDefinition> fields = new ArrayList<>();

    public boolean isFixedLength() {
        return fixedLength;
    }

    public void setFixedLength(boolean fixedLength) {
        this.fixedLength = fixedLength;
    }

    public int getTotalLength() {
        return totalLength;
    }

    public void setTotalLength(int totalLength) {
        this.totalLength = totalLength;
    }

    public List<FieldDefinition> getFields() {
        return fields;
    }

    public void addField(FieldDefinition field) {
        fields.add(field);
    }
}

class FieldDefinition {
    private String name;
    private int startPos;
    private int length;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStartPos() {
        return startPos;
    }

    public void setStartPos(int startPos) {
        this.startPos = startPos;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}

public class JsonConfigurableFileParser {
    private LayoutConfig layoutConfig;

    public JsonConfigurableFileParser(String layoutJsonPath) throws IOException {
        this.layoutConfig = LayoutConfig.fromJsonFile(layoutJsonPath);
    }

    public List<DataRecord> parseFile(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            return parseReader(reader);
        }
    }

    public List<DataRecord> parseString(String content) throws IOException {
        try (BufferedReader reader = new BufferedReader(new StringReader(content))) {
            return parseReader(reader);
        }
    }

    private List<DataRecord> parseReader(BufferedReader reader) throws IOException {
        List<DataRecord> records = new ArrayList<>();
        DataRecord currentRecord = null;

        String line;
        while ((line = reader.readLine()) != null) {
            // Skip empty lines
            if (line.trim().isEmpty()) {
                continue;
            }

            if (line.startsWith("HEADER")) {
                // Start a new record
                if (currentRecord != null && currentRecord.header != null) {
                    records.add(currentRecord);
                }
                currentRecord = new DataRecord();
                currentRecord.header = parseHeaderRecord(line.substring("HEADER".length()));
            } else if (line.startsWith("BASE")) {
                if (currentRecord == null) {
                    currentRecord = new DataRecord();
                }

                // Parse BASE record
                String baseData = line.substring("BASE".length());
                currentRecord.base = parseBaseRecord(baseData);

                // Parse segments
                RecordLayout baseLayout = layoutConfig.getLayout("BASE");
                int segmentStartPos = 0;
                
                // If BASE has a fixed layout, calculate where segments begin
                if (baseLayout != null && baseLayout.isFixedLength()) {
                    segmentStartPos = baseLayout.getTotalLength();
                } else {
                    // Default to sum of field lengths if not specified
                    for (FieldDefinition field : baseLayout.getFields()) {
                        segmentStartPos = Math.max(segmentStartPos, field.getStartPos() + field.getLength());
                    }
                }
                
                // Parse the segments from the remaining part of the line
                parseSegments(baseData, segmentStartPos, currentRecord.base);
            }
        }

        // Add the last record if it exists
        if (currentRecord != null && currentRecord.header != null) {
            records.add(currentRecord);
        }

        return records;
    }

    private HeaderRecord parseHeaderRecord(String data) {
        HeaderRecord record = new HeaderRecord();
        
        RecordLayout layout = layoutConfig.getLayout("HEADER");
        if (layout != null) {
            for (FieldDefinition field : layout.getFields()) {
                int end = Math.min(field.getStartPos() + field.getLength(), data.length());
                if (field.getStartPos() < data.length()) {
                    String value = data.substring(field.getStartPos(), end).trim();
                    record.setField(field.getName(), value);
                }
            }
        }
        
        return record;
    }

    private BaseRecord parseBaseRecord(String data) {
        BaseRecord record = new BaseRecord();
        
        RecordLayout layout = layoutConfig.getLayout("BASE");
        if (layout != null) {
            for (FieldDefinition field : layout.getFields()) {
                int end = Math.min(field.getStartPos() + field.getLength(), data.length());
                if (field.getStartPos() < data.length()) {
                    String value = data.substring(field.getStartPos(), end).trim();
                    record.setField(field.getName(), value);
                }
            }
        }
        
        return record;
    }

    private void parseSegments(String data, int startPos, BaseRecord baseRecord) {
        int currentPos = startPos;

        while (currentPos + 2 <= data.length()) {  // Need at least 2 chars for segment type
            // Try to identify segment type (J1, K2, etc.)
            String potentialType = data.substring(currentPos, currentPos + 2);
            
            if (layoutConfig.hasLayout(potentialType)) {
                RecordLayout segmentLayout = layoutConfig.getLayout(potentialType);
                
                // Check if we have enough data for this segment
                if (segmentLayout.isFixedLength()) {
                    if (currentPos + segmentLayout.getTotalLength() <= data.length()) {
                        // Extract the segment data
                        String segmentData = data.substring(currentPos, currentPos + segmentLayout.getTotalLength());
                        SegmentRecord segment = parseSegmentRecord(segmentData, potentialType);
                        baseRecord.getOrCreateList(potentialType).add(segment);
                        
                        // Move past this segment
                        currentPos += segmentLayout.getTotalLength();
                    } else {
                        // Not enough data for complete segment
                        break;
                    }
                } else {
                    // For variable-length segments, we need to calculate the length
                    int maxEnd = currentPos;
                    for (FieldDefinition field : segmentLayout.getFields()) {
                        maxEnd = Math.max(maxEnd, currentPos + field.getStartPos() + field.getLength());
                    }
                    
                    if (maxEnd <= data.length()) {
                        String segmentData = data.substring(currentPos, maxEnd);
                        SegmentRecord segment = parseSegmentRecord(segmentData, potentialType);
                        baseRecord.getOrCreateList(potentialType).add(segment);
                        
                        // Move past this segment
                        currentPos = maxEnd;
                    } else {
                        // Not enough data
                        break;
                    }
                }
            } else {
                // Not a recognized segment type, try next position
                currentPos++;
            }
        }
    }

    private SegmentRecord parseSegmentRecord(String data, String type) {
        SegmentRecord record = new SegmentRecord();
        record.type = type;
        
        RecordLayout layout = layoutConfig.getLayout(type);
        if (layout != null) {
            for (FieldDefinition field : layout.getFields()) {
                int end = Math.min(field.getStartPos() + field.getLength(), data.length());
                if (field.getStartPos() < data.length()) {
                    String value = data.substring(field.getStartPos(), end).trim();
                    record.setField(field.getName(), value);
                }
            }
        }
        
        return record;
    }

    public static void main(String[] args) {
        // Example JSON layout configuration
        String jsonConfig = "{\n" +
                "  \"HEADER\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 6,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 0, \"length\": 2 },\n" +
                "      \"name\": { \"startPos\": 2, \"length\": 4 }\n" +
                "    }\n" +
                "  },\n" +
                "  \"BASE\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 6,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 0, \"length\": 2 },\n" +
                "      \"id1\": { \"startPos\": 2, \"length\": 4 }\n" +
                "    }\n" +
                "  },\n" +
                "  \"J1\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 8,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 2, \"length\": 2 },\n" +
                "      \"name\": { \"startPos\": 4, \"length\": 4 }\n" +
                "    }\n" +
                "  },\n" +
                "  \"J2\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 8,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 2, \"length\": 2 },\n" +
                "      \"name\": { \"startPos\": 4, \"length\": 4 }\n" +
                "    }\n" +
                "  },\n" +
                "  \"K1\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 8,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 2, \"length\": 2 },\n" +
                "      \"name\": { \"startPos\": 4, \"length\": 4 }\n" +
                "    }\n" +
                "  },\n" +
                "  \"K2\": {\n" +
                "    \"fixedLength\": true,\n" +
                "    \"totalLength\": 8,\n" +
                "    \"fields\": {\n" +
                "      \"id\": { \"startPos\": 2, \"length\": 2 },\n" +
                "      \"name\": { \"startPos\": 4, \"length\": 4 }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        try {
            // Write the JSON configuration to a temporary file
            String configPath = "layout_config.json";
            Files.write(Paths.get(configPath), jsonConfig.getBytes());

            // Sample input with various segment types
            String sampleInput = "HEADER01ABCD\r\n" +
                                 "BASE02EFGHJ103IJKLJ104MNOPK105QRSTK206UVWX\r\n" +
                                 "HEADER07YZAB\r\n" +
                                 "BASE08CDEFJ109GHIJK210LMNOK311OPQRK412STUV\r\n";

            System.out.println("Sample Layout Configuration:");
            System.out.println(jsonConfig);
            
            System.out.println("\nSample Input File Content:");
            System.out.println(sampleInput);
            
            // Create the parser with the layout configuration
            JsonConfigurableFileParser parser = new JsonConfigurableFileParser(configPath);
            
            // Parse the input
            List<DataRecord> records = parser.parseString(sampleInput);
            
            System.out.println("\nParsed Output:");
            for (int i = 0; i < records.size(); i++) {
                System.out.println("Record " + (i + 1) + ":");
                System.out.println(records.get(i));
            }
            
            // Clean up
            Files.delete(Paths.get(configPath));
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
