package eu.socialsensor.utils;

import com.alibaba.fastjson.JSON;
import com.baidu.hugegraph.schema.SchemaManager;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import eu.socialsensor.insert.Custom;
import eu.socialsensor.insert.InsertionBase;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.parboiled.common.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.parboiled.common.Preconditions.*;
import static eu.socialsensor.utils.JanusGraphUtils.*;

/**
 *  @author: liyu04
 *  @date: 2020/3/10
 *  @version: V1.0
 *
 * @Description: 处理态势数据
 */
public class TaiShiDataUtils implements Custom
{

    public static final Logger LOG = LogManager.getLogger(TaiShiDataUtils.class);
    public static final String IP = "ip";
    public static final String WRITE_DATE = "write_date";
    public static final String KILL_CHAIN = "kill_chain";
    public static final String SEVERITY = "severity";
    public static final String VICTIM_TYPE = "victim_type";
    public static final String ATTACK_RESULT = "attack_result";
    public static final String ATTACK_TYPE_ID = "attack_type_id";
    public static final String ATTACK_TYPE = "attack_type";
    public static final String RULE_ID = "rule_id";
    public static final String RULE_NAME = "rule_name";
    public static final String RULE_VERSION = "rule_version";
    public static final String VULN_DESC = "vuln_desc";
    public static final String VULN_HARM = "vuln_harm";
    public static final String VULN_NAME = "vuln_name";
    public static final String VULN_TYPE = "vuln_type";
    public static final String URI = "uri";
    public static final String SERVER = "server";
    public static final String SIP = "sip";
    public static final String DIP = "dip";
    public static final String ATTACKER = "attacker";
    //ATTACK_EDGE is vertex label,use vertex descript edge
    public static final String ATTACK_EDGE = "attack";
    public static final String VICTIM = "victim";
    public static final String DPORT = "dport";
    public static final String IPTOATT = "iptoatt";
    public static final String IPTOSIP = "iptosip";
    public static final String STOATT = "stoatt";
    public static final String ATOATT = "atoatt";
    public static final String ATODPORT = "atodport";
    public static final String DIPTOVIC = "diptovic";
    public static final String DPORTTOVIC = "dporttovic";
    public static final String IPTOVIC = "iptovic";
    public static final String IPTODIP = "iptodip";


    public TaiShiDataUtils()
    {
    }

    /**
     * for hugegraph create schema
     * @param huge
     */
    public void createSchema(SchemaManager huge)
    {
        huge.propertyKey(IP).asText().ifNotExist().create();
        huge.propertyKey(WRITE_DATE).asLong().ifNotExist().create();
        huge.propertyKey(KILL_CHAIN).asText().ifNotExist().create();
        huge.propertyKey(SEVERITY).asInt().ifNotExist().create();
        huge.propertyKey(VICTIM_TYPE).asText().ifNotExist().create();
        huge.propertyKey(ATTACK_RESULT).asText().ifNotExist().create();
        huge.propertyKey(ATTACK_TYPE_ID).asInt().ifNotExist().create();
        huge.propertyKey(ATTACK_TYPE).asText().ifNotExist().create();
        huge.propertyKey(RULE_ID).asInt().ifNotExist().create();
        huge.propertyKey(RULE_NAME).asText().ifNotExist().create();
        huge.propertyKey(RULE_VERSION).asInt().ifNotExist().create();
        huge.propertyKey(VULN_DESC).asText().ifNotExist().create();
        huge.propertyKey(VULN_HARM).asText().ifNotExist().create();
        huge.propertyKey(VULN_NAME).asText().ifNotExist().create();
        huge.propertyKey(VULN_TYPE).asText().ifNotExist().create();
        huge.propertyKey(DPORT).asInt().ifNotExist().create();
        huge.propertyKey(URI).asText().ifNotExist().create();

        huge.vertexLabel(SERVER).properties(IP).useCustomizeStringId().ifNotExist().create();
        huge.vertexLabel(SIP).properties(IP).useCustomizeStringId().ifNotExist().create();
        huge.vertexLabel(DIP).properties(IP).useCustomizeStringId().ifNotExist().create();
        huge.vertexLabel(ATTACKER).properties(IP).useCustomizeStringId().ifNotExist().create();
        huge.vertexLabel(ATTACK_EDGE).properties(ATTACK_TYPE_ID,ATTACK_RESULT,KILL_CHAIN,SEVERITY,VICTIM_TYPE,
                WRITE_DATE,ATTACK_TYPE,RULE_ID,RULE_NAME,RULE_VERSION,
                VULN_DESC,VULN_HARM,VULN_NAME,VULN_TYPE,URI).
                nullableKeys(ATTACK_TYPE_ID,ATTACK_RESULT,KILL_CHAIN,SEVERITY,VICTIM_TYPE,
                        WRITE_DATE,ATTACK_TYPE,RULE_ID,RULE_NAME,RULE_VERSION,
                        VULN_DESC,VULN_HARM,VULN_NAME,VULN_TYPE,URI).
                useAutomaticId().ifNotExist().create();
        huge.vertexLabel(DPORT).properties(DPORT).useCustomizeStringId().ifNotExist().create();
        huge.vertexLabel(VICTIM).properties(IP).useCustomizeStringId().ifNotExist().create();

        huge.edgeLabel(IPTOATT).link(SERVER, ATTACKER).ifNotExist().create();
        huge.edgeLabel(IPTOSIP).link(SERVER, SIP).ifNotExist().create();
        huge.edgeLabel(STOATT).link(SIP, ATTACKER).ifNotExist().create();
        huge.edgeLabel(ATOATT).link(ATTACKER, ATTACK_EDGE).properties(WRITE_DATE).ifNotExist().create();
        huge.edgeLabel(ATODPORT).link(ATTACK_EDGE, DPORT).properties(WRITE_DATE).ifNotExist().create();
        huge.edgeLabel(DPORTTOVIC).link(DPORT, VICTIM).ifNotExist().create();
        huge.edgeLabel(DIPTOVIC).link(DIP, VICTIM).ifNotExist().create();
        huge.edgeLabel(IPTODIP).link(SERVER, DIP).ifNotExist().create();
        huge.edgeLabel(IPTOVIC).link(SERVER, VICTIM).ifNotExist().create();

        huge.indexLabel("attack_type_id").onV(ATTACK_EDGE).by(ATTACK_TYPE_ID).secondary().ifNotExist().create();
        huge.indexLabel("rule_id").onV(ATTACK_EDGE).by(RULE_ID).secondary().ifNotExist().create();
        huge.indexLabel("uri").onV(ATTACK_EDGE).by(URI).secondary().ifNotExist().create();
        huge.indexLabel("write_date").onV(ATTACK_EDGE).by(WRITE_DATE).range().ifNotExist().create();
        huge.indexLabel("edge_write_date").onE(ATOATT).by(WRITE_DATE).range().ifNotExist().create();
    }

    private static Map<String, Pair<String, String>> edgeLabelMap = new HashMap<>();
    static {
        edgeLabelMap.put(IPTOATT, Pair.of(SERVER, ATTACKER));
        edgeLabelMap.put(IPTOSIP, Pair.of(SERVER, SIP));
        edgeLabelMap.put(STOATT, Pair.of(SIP, ATTACKER));
        edgeLabelMap.put(ATOATT, Pair.of(ATTACKER, ATTACK_EDGE));
        edgeLabelMap.put(ATODPORT, Pair.of(ATTACK_EDGE, DPORT));
        edgeLabelMap.put(DPORTTOVIC, Pair.of(DPORT, VICTIM));
        edgeLabelMap.put(DIPTOVIC, Pair.of(DIP, VICTIM));
        edgeLabelMap.put(IPTODIP, Pair.of(SERVER, DIP));
        edgeLabelMap.put(IPTOVIC, Pair.of(SERVER, VICTIM));
    }
    public static String getVertexLabel(String edgelabel,boolean left)
    {
        if (left) {
            return edgeLabelMap.get(edgelabel).getLeft();
        } else {
            return edgeLabelMap.get(edgelabel).getRight();
        }
    }

    /**
     * for janusgraph create schema
     * @param janus
     */
    public void createSchema(JanusGraphManagement janus)
    {
        PropertyKey ip = getOrCreatePropertyKey(janus, IP, String.class, Cardinality.SINGLE);
        PropertyKey write_date = getOrCreatePropertyKey(janus, WRITE_DATE, Long.class, Cardinality.SINGLE);
        PropertyKey kill_chain = getOrCreatePropertyKey(janus, KILL_CHAIN, String.class, Cardinality.SINGLE);
        PropertyKey serverity = getOrCreatePropertyKey(janus, SEVERITY, Integer.class, Cardinality.SINGLE);
        PropertyKey victim_type = getOrCreatePropertyKey(janus, VICTIM_TYPE, String.class, Cardinality.SINGLE);
        PropertyKey attack_result = getOrCreatePropertyKey(janus, ATTACK_RESULT, String.class,
                                                           Cardinality.SINGLE);
        PropertyKey attack_type_id = getOrCreatePropertyKey(janus, ATTACK_TYPE_ID,
                                                            Integer.class, Cardinality.SINGLE);
        PropertyKey attack_type = getOrCreatePropertyKey(janus, ATTACK_TYPE, String.class, Cardinality.SINGLE);
        PropertyKey rule_id = getOrCreatePropertyKey(janus, RULE_ID, Integer.class, Cardinality.SINGLE);
        PropertyKey rule_name = getOrCreatePropertyKey(janus, RULE_NAME, String.class, Cardinality.SINGLE);
        PropertyKey rule_version = getOrCreatePropertyKey(janus, RULE_VERSION, Integer.class,
                                                          Cardinality.SINGLE);
        PropertyKey vuln_desc = getOrCreatePropertyKey(janus, VULN_DESC, String.class, Cardinality.SINGLE);
        PropertyKey vuln_harm = getOrCreatePropertyKey(janus, VULN_HARM, String.class, Cardinality.SINGLE);
        PropertyKey vuln_name = getOrCreatePropertyKey(janus, VULN_NAME, String.class, Cardinality.SINGLE);
        PropertyKey vuln_type = getOrCreatePropertyKey(janus, VULN_TYPE, String.class, Cardinality.SINGLE);
        PropertyKey dport = getOrCreatePropertyKey(janus, DPORT, Integer.class, Cardinality.SINGLE);
        PropertyKey uri = getOrCreatePropertyKey(janus, URI, String.class, Cardinality.SINGLE);

        //vertex label
        VertexLabel server = (VertexLabel) createLabel(janus, SERVER, true);
        createLabel(janus, SIP, true);
        createLabel(janus, DIP, true);
        createLabel(janus, ATTACKER, true);
        VertexLabel attack_edge = (VertexLabel) createLabel(janus, ATTACK_EDGE, true);
        createLabel(janus, DPORT, true);
        createLabel(janus, VICTIM, true);
        createLabel(janus, DIP, true);
        //edge label
        createLabel(janus, IPTOATT, false);
        createLabel(janus, IPTOSIP, false);
        createLabel(janus, IPTODIP, false);
        createLabel(janus, IPTOVIC, false);
        createLabel(janus, STOATT, false);
        EdgeLabel atoatt = (EdgeLabel) createLabel(janus, ATOATT, false);
        createLabel(janus, ATODPORT, false);
        createLabel(janus, DPORTTOVIC, false);
        createLabel(janus, DIPTOVIC, false);

        buildVertexCompositeIndex(janus, "ip", true, server, ip);
        buildVertexCompositeIndex(janus, "attack_type_id", false,
                                  attack_edge, attack_type_id);
        buildVertexCompositeIndex(janus, "rule_id", false, attack_edge, rule_id);
        buildVertexCompositeIndex(janus, "uri", false, attack_edge, uri);
        buildVertexCompositeIndex(janus, "write_date", false, attack_edge, write_date);
        janus.buildIndex("edge_write_date", Edge.class).indexOnly(atoatt)
             .addKey(write_date).buildCompositeIndex();
        janus.commit();
    }



    /**
     * read file return instance
     * @param fileOrDir
     * @return
     */
    @Override
    public Iterator<Object> readLine(File fileOrDir) throws IOException
    {
        checkArgNotNull(fileOrDir, "data file is null!");
        if (fileOrDir.isFile()) {
            return new Iter(fileOrDir);
        } else if (fileOrDir.isDirectory()) {
            Iterator<File> fileIterator = FileUtils.iterateFiles(fileOrDir,
                    new String[]{"txt"},
                    false);
            return new Iter(fileIterator);
        } else {
            throw new IOException("can not find data files");
        }
    }

    private static LineIterator lineIterator(File file) throws IOException
    {
        return FileUtils.lineIterator(file, "UTF-8");
    }

    /**
     * the instance data of taishi graph
     */
    public static class TaiShiDataset
    {

        private String agent;
        private String attack_flag;
        private String attack_result;
        private String attack_type;
        private int	attack_type_id;
        private String	attacker;
        private String	code_language;
        private int	confidence;
        private String	cookie;
        private String	detail_info;
        private String	device_ip;
        private String	dip;
        private int	dolog_count;
        private int	dport;
        private String	event_id;
        private String	file_name;
        private String	hive_partition_time;
        private String	host;
        private String	kill_chain;
        private String	method;
        private String	parameter;
        private String	public_date;
        private String	referer;
        private String	req_body;
        private String	req_header;
        private String	rsp_body;
        private int	rsp_body_len;
        private int	rsp_content_length;
        private String	rsp_content_type;
        private String	rsp_header;
        private int	rsp_status;
        private int	rule_id;
        private String	rule_name;
        private int	rule_version;
        private String	serial_num;
        private String	sess_id;
        private int	severity;
        private String	sip;
        private String	site_app;
        private String	solution;
        private int	sport;
        private String	uri;
        private String	vendor_id;
        private String	victim;
        private String	victim_type;
        private String	vuln_desc;
        private String	vuln_harm;
        private String	vuln_name;
        private String	vuln_type;
        private String	webrules_tag;
        private long write_date;

        public TaiShiDataset()
        {
        }

        public String getAgent()
        {
            return agent;
        }

        public void setAgent(String agent)
        {
            this.agent = agent;
        }

        public String getAttack_flag()
        {
            return attack_flag;
        }

        public void setAttack_flag(String attack_flag)
        {
            this.attack_flag = attack_flag;
        }

        public String getAttack_result()
        {
            return attack_result;
        }

        public void setAttack_result(String attack_result)
        {
            this.attack_result = attack_result;
        }

        public String getAttack_type()
        {
            return attack_type;
        }

        public void setAttack_type(String attack_type)
        {
            this.attack_type = attack_type;
        }

        public int getAttack_type_id()
        {
            return attack_type_id;
        }

        public void setAttack_type_id(int attack_type_id)
        {
            this.attack_type_id = attack_type_id;
        }

        public String getAttacker()
        {
            return attacker;
        }

        public void setAttacker(String attacker)
        {
            this.attacker = attacker;
        }

        public String getCode_language()
        {
            return code_language;
        }

        public void setCode_language(String code_language)
        {
            this.code_language = code_language;
        }

        public int getConfidence()
        {
            return confidence;
        }

        public void setConfidence(int confidence)
        {
            this.confidence = confidence;
        }

        public String getCookie()
        {
            return cookie;
        }

        public void setCookie(String cookie)
        {
            this.cookie = cookie;
        }

        public String getDetail_info()
        {
            return detail_info;
        }

        public void setDetail_info(String detail_info)
        {
            this.detail_info = detail_info;
        }

        public String getDevice_ip()
        {
            return device_ip;
        }

        public void setDevice_ip(String device_ip)
        {
            this.device_ip = device_ip;
        }

        public String getDip()
        {
            return dip;
        }

        public void setDip(String dip)
        {
            this.dip = dip;
        }

        public int getDolog_count()
        {
            return dolog_count;
        }

        public void setDolog_count(int dolog_count)
        {
            this.dolog_count = dolog_count;
        }

        public int getDport()
        {
            return dport;
        }

        public void setDport(int dport)
        {
            this.dport = dport;
        }

        public String getEvent_id()
        {
            return event_id;
        }

        public void setEvent_id(String event_id)
        {
            this.event_id = event_id;
        }

        public String getFile_name()
        {
            return file_name;
        }

        public void setFile_name(String file_name)
        {
            this.file_name = file_name;
        }

        public String getHive_partition_time()
        {
            return hive_partition_time;
        }

        public void setHive_partition_time(String hive_partition_time)
        {
            this.hive_partition_time = hive_partition_time;
        }

        public String getHost()
        {
            return host;
        }

        public void setHost(String host)
        {
            this.host = host;
        }

        public String getKill_chain()
        {
            return kill_chain;
        }

        public void setKill_chain(String kill_chain)
        {
            this.kill_chain = kill_chain;
        }

        public String getMethod()
        {
            return method;
        }

        public void setMethod(String method)
        {
            this.method = method;
        }

        public String getParameter()
        {
            return parameter;
        }

        public void setParameter(String parameter)
        {
            this.parameter = parameter;
        }

        public String getPublic_date()
        {
            return public_date;
        }

        public void setPublic_date(String public_date)
        {
            this.public_date = public_date;
        }

        public String getReferer()
        {
            return referer;
        }

        public void setReferer(String referer)
        {
            this.referer = referer;
        }

        public String getReq_body()
        {
            return req_body;
        }

        public void setReq_body(String req_body)
        {
            this.req_body = req_body;
        }

        public String getReq_header()
        {
            return req_header;
        }

        public void setReq_header(String req_header)
        {
            this.req_header = req_header;
        }

        public String getRsp_body()
        {
            return rsp_body;
        }

        public void setRsp_body(String rsp_body)
        {
            this.rsp_body = rsp_body;
        }

        public int getRsp_body_len()
        {
            return rsp_body_len;
        }

        public void setRsp_body_len(int rsp_body_len)
        {
            this.rsp_body_len = rsp_body_len;
        }

        public int getRsp_content_length()
        {
            return rsp_content_length;
        }

        public void setRsp_content_length(int rsp_content_length)
        {
            this.rsp_content_length = rsp_content_length;
        }

        public String getRsp_content_type()
        {
            return rsp_content_type;
        }

        public void setRsp_content_type(String rsp_content_type)
        {
            this.rsp_content_type = rsp_content_type;
        }

        public String getRsp_header()
        {
            return rsp_header;
        }

        public void setRsp_header(String rsp_header)
        {
            this.rsp_header = rsp_header;
        }

        public int getRsp_status()
        {
            return rsp_status;
        }

        public void setRsp_status(int rsp_status)
        {
            this.rsp_status = rsp_status;
        }

        public int getRule_id()
        {
            return rule_id;
        }

        public void setRule_id(int rule_id)
        {
            this.rule_id = rule_id;
        }

        public String getRule_name()
        {
            return rule_name;
        }

        public void setRule_name(String rule_name)
        {
            this.rule_name = rule_name;
        }

        public int getRule_version()
        {
            return rule_version;
        }

        public void setRule_version(int rule_version)
        {
            this.rule_version = rule_version;
        }

        public String getSerial_num()
        {
            return serial_num;
        }

        public void setSerial_num(String serial_num)
        {
            this.serial_num = serial_num;
        }

        public String getSess_id()
        {
            return sess_id;
        }

        public void setSess_id(String sess_id)
        {
            this.sess_id = sess_id;
        }

        public int getSeverity()
        {
            return severity;
        }

        public void setSeverity(int severity)
        {
            this.severity = severity;
        }

        public String getSip()
        {
            return sip;
        }

        public void setSip(String sip)
        {
            this.sip = sip;
        }

        public String getSite_app()
        {
            return site_app;
        }

        public void setSite_app(String site_app)
        {
            this.site_app = site_app;
        }

        public String getSolution()
        {
            return solution;
        }

        public void setSolution(String solution)
        {
            this.solution = solution;
        }

        public int getSport()
        {
            return sport;
        }

        public void setSport(int sport)
        {
            this.sport = sport;
        }

        public String getUri()
        {
            return uri;
        }

        public void setUri(String uri)
        {
            this.uri = uri;
        }

        public String getVendor_id()
        {
            return vendor_id;
        }

        public void setVendor_id(String vendor_id)
        {
            this.vendor_id = vendor_id;
        }

        public String getVictim()
        {
            return victim;
        }

        public void setVictim(String victim)
        {
            this.victim = victim;
        }

        public String getVictim_type()
        {
            return victim_type;
        }

        public void setVictim_type(String victim_type)
        {
            this.victim_type = victim_type;
        }

        public String getVuln_desc()
        {
            return vuln_desc;
        }

        public void setVuln_desc(String vuln_desc)
        {
            this.vuln_desc = vuln_desc;
        }

        public String getVuln_harm()
        {
            return vuln_harm;
        }

        public void setVuln_harm(String vuln_harm)
        {
            this.vuln_harm = vuln_harm;
        }

        public String getVuln_name()
        {
            return vuln_name;
        }

        public void setVuln_name(String vuln_name)
        {
            this.vuln_name = vuln_name;
        }

        public String getVuln_type()
        {
            return vuln_type;
        }

        public void setVuln_type(String vuln_type)
        {
            this.vuln_type = vuln_type;
        }

        public String getWebrules_tag()
        {
            return webrules_tag;
        }

        public void setWebrules_tag(String webrules_tag)
        {
            this.webrules_tag = webrules_tag;
        }

        public long getWrite_date()
        {
            return write_date;
        }

        public void setWrite_date(long write_date)
        {
            this.write_date = write_date;
        }

        private static TaiShiDataset fromJson(String line)
        {
            return JSON.parseObject(line, TaiShiDataset.class);
        }
    }

    /**
     *
     */
    public static class Iter implements Iterator<Object>
    {

        private LineIterator lineIterator;
        private Iterator<File> fileIterator;
        private Iter(File file) throws IOException
        {
            this.fileIterator = null;
            this.lineIterator = lineIterator(file);
        }

        private Iter(Iterator<File> fileIterator) throws IOException
        {
            this.fileIterator = fileIterator;
            Preconditions.checkArgument(fileIterator.hasNext(),"taishi files is empty");
            this.lineIterator = (FileUtils.lineIterator(fileIterator.next(), "UTF-8"));
        }

        private boolean resetIter() throws IOException
        {
            while (fileIterator.hasNext()) {
                lineIterator = (FileUtils.lineIterator(fileIterator.next(),"UTF-8"));
                if (lineIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean hasNext()
        {
            try {
                return lineIterator.hasNext() || (fileIterator!= null && resetIter());
            } catch (IOException e) {
                LOG.error("read taishi data file error !");
            }
            return false;
        }

        @Override
        public TaiShiDataset next()
        {
            return TaiShiDataset.fromJson(lineIterator.nextLine());
        }
    }

    /**
     * write data into graph,and mertic timer
     * @param lineData
     * @param insertionBase
     * @param <T>
     */
    @Override
    public <T,E> void writeData(Object lineData,InsertionBase<T,E> insertionBase)
    {
        Preconditions.checkArgNotNull(lineData, "taishi data line is null");
        TaiShiDataset line = (TaiShiDataset) lineData;
        Timer getOrCreateTimes = insertionBase.getOrCreateTimes;
        Timer relateNodesTimes = insertionBase.relateNodesTimes;

        E ipAttacker_v;
        E attacker_v;
        E ipSip_v;
        E sip_v;
        E ipV_v;
        E victim_v;
        E ipDip_v;
        E dip_v;
        E dport_v;
        E attack_edge_v;

        Timer.Context contextAtt = getOrCreateTimes.time();
        try {
            String attacker = line.getAttacker().trim();
            ipAttacker_v = insertionBase.getOrCreateCust(
                    SERVER, attacker, ImmutableMap.of(IP, attacker));
            attacker_v = insertionBase.getOrCreateCust(
                    ATTACKER, String.format("A%s",attacker),  ImmutableMap.of(IP, attacker));

            String sip = line.getSip().trim();
            ipSip_v = insertionBase.getOrCreateCust(
                    SERVER, sip, ImmutableMap.of(IP, sip));
            sip_v = insertionBase.getOrCreateCust(
                    SIP, String.format("S%s", sip), ImmutableMap.of(IP, sip));
        }finally {
            contextAtt.stop();
        }

        Timer.Context contextVic = getOrCreateTimes.time();
        try {
            String victim = line.getVictim().trim();
            ipV_v = insertionBase.getOrCreateCust(
                    SERVER, victim, ImmutableMap.of(IP, victim));
            victim_v = insertionBase.getOrCreateCust(
                    VICTIM, String.format("V%s",victim), ImmutableMap.of(IP, victim));

            String dip = line.getDip().trim();
            ipDip_v = insertionBase.getOrCreateCust(
                    SERVER, dip, ImmutableMap.of(IP, dip));
            dip_v = insertionBase.getOrCreateCust(
                    DIP, String.format("D%s",dip), ImmutableMap.of(IP, dip));
            int dport = line.getDport();
            dport_v = insertionBase.getOrCreateCust(
                    DPORT,
                    String.format("%s:%d",victim,dport),
                    ImmutableMap.of(DPORT, dport));
        }finally {
            contextVic.stop();
        }

        ImmutableMap<String, Object> attack_edge = ImmutableMap
                .<String, Object>builder()
                .put(WRITE_DATE, line.getWrite_date())
                .put(KILL_CHAIN, line.getKill_chain())
                .put(SEVERITY, line.getSeverity())
                .put(VICTIM_TYPE, line.getVictim_type())
                .put(ATTACK_RESULT, line.getAttack_result())
                .put(ATTACK_TYPE_ID, line.getAttack_type_id())
                .put(ATTACK_TYPE, line.getAttack_type())
                .put(RULE_ID, line.getRule_id())
                .put(RULE_NAME, line.getRule_name())
                .put(RULE_VERSION, line.getRule_version())
                .put(VULN_DESC, line.getVuln_desc())
                .put(VULN_HARM, line.getVuln_harm())
                .put(VULN_NAME, line.getVuln_name())
                .put(VULN_TYPE, line.getVuln_type())
                .put(URI, line.getUri()).build();

        Timer.Context contextAttEdge = getOrCreateTimes.time();
        try {
            attack_edge_v = insertionBase.getOrCreateCust(ATTACK_EDGE, null, attack_edge);
        }finally {
            contextAttEdge.stop();
        }

        ImmutableMap<String, Object> write_date_properties = ImmutableMap.of(WRITE_DATE, line.getWrite_date());
        Timer.Context contextEdge = relateNodesTimes.time();
        try {
            insertionBase.relateNodesCust(IPTOATT, ipAttacker_v, attacker_v, null);
            insertionBase.relateNodesCust(IPTOSIP, ipSip_v, sip_v, null);
            insertionBase.relateNodesCust(STOATT, sip_v, attacker_v, null);
            insertionBase.relateNodesCust(IPTOVIC, ipV_v, victim_v, null);
            insertionBase.relateNodesCust(IPTODIP, ipDip_v, dip_v, null);
            insertionBase.relateNodesCust(DIPTOVIC, dip_v, victim_v, null);
            insertionBase.relateNodesCust(DPORTTOVIC, dport_v, victim_v, null);
            insertionBase.relateNodesCust(
                    ATOATT, attacker_v, attack_edge_v,
                    write_date_properties
            );
            insertionBase.relateNodesCust(ATODPORT, attack_edge_v, dport_v, write_date_properties);
        }finally {
            contextEdge.stop();
        }
    }

    @Override
    public void createSchema(Object graph, GraphDatabaseType type)
    {
        switch (type) {
            case HUGEGRAPH_CORE:
                createSchema((SchemaManager) graph);
                break;
            case JANUSGRAPH_CORE:
                createSchema((JanusGraphManagement) graph);
                break;
            default:
                throw new RuntimeException(String.format("The graph database type %s is not regist!"));
        }
    }

}
