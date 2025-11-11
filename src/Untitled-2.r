#include <omp.h>
#include <bits/stdc++.h>
using namespace std;

// ---------------- CONFIGURATION ----------------
const double LAT_BIN_SIZE = 1.0;
const double LON_BIN_SIZE = 1.0;
const double MAX_ICON_SCALE = 5.5;
const double MIN_ICON_SCALE = 0.8;
// ------------------------------------------------

struct PairHash {
    size_t operator()(const pair<int,int>&p) const noexcept {
        return (uint64_t((uint32_t)p.first) << 32) ^ (uint32_t)p.second;
    }
};

struct Event {
    string date, raw_time, type;
    double lat = NAN, lon = NAN, mag = NAN;
    bool valid = false;
};

static inline string trim(const string &s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}
static inline string toLower(string s) {
    for (char &c : s) c = tolower(c);
    return s;
}
vector<string> csvSplit(const string &line) {
    vector<string> out; string cur; bool inq = false;
    for (char c : line) {
        if (c == '"') { inq = !inq; continue; }
        if (c == ',' && !inq) { out.push_back(trim(cur)); cur.clear(); }
        else cur.push_back(c);
    }
    out.push_back(trim(cur));
    return out;
}
bool parseDouble(const string &s, double &v) {
    try { v = stod(s); return true; } catch (...) { return false; }
}
string extractDate(const string &t) {
    if (t.size() >= 10) return t.substr(0, 10);
    return t;
}

#ifdef _WIN32
void createOutputDir() { system("if not exist output mkdir output"); }
#else
void createOutputDir() { system("mkdir -p output 2> /dev/null"); }
#endif

// ============================================================================
// MAIN
// ============================================================================
int main(int argc, char* argv[]) {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    createOutputDir();

    // --- COLORS ---
    const string RESET  = "\033[0m";
    const string BOLD   = "\033[1m";
    const string CYAN   = "\033[96m";
    const string GREEN  = "\033[92m";
    const string RED    = "\033[91m";
    const string YELLOW = "\033[93m";
    const string BLUE   = "\033[94m";
    const string MAGENTA= "\033[95m";
    const string GRAY   = "\033[90m";
    const string WHITE  = "\033[97m";

    string infile = "data/events_12months.csv";
    int threads = omp_get_max_threads();
    if (argc >= 2) infile = argv[1];
    if (argc >= 3) threads = max(1, atoi(argv[2]));
    omp_set_num_threads(max(1, threads));

    // HEADER
    cout << BOLD << CYAN;
    cout << "+======================================================+\n";
    cout << "|           PARALLEL GLOBAL DISASTER ANALYZER          |\n";
    cout << "+======================================================+\n" << RESET << "\n";

    cout << WHITE << "  Input File   : " << GREEN << infile << "\n";
    cout << WHITE << "  Threads Used : " << YELLOW << threads << "\n\n" << RESET;

    // READ CSV
    ifstream fin(infile);
    if (!fin.is_open()) {
        cerr << RED << "[ERROR]" << RESET << " Cannot open input file: " << infile << "\n";
        cerr << "        Please run: python fetch_12month_data.py\n";
        return 1;
    }

    vector<string> lines; string line;
    while (getline(fin, line)) if (!line.empty()) lines.push_back(line);
    fin.close();

    if (lines.size() <= 1) {
        cerr << RED << "[ERROR]" << RESET << " CSV file empty or invalid.\n";
        return 1;
    }

    vector<string> header = csvSplit(lines[0]);
    int idx_time=-1, idx_lat=-1, idx_lon=-1, idx_mag=-1, idx_type=-1;
    for (int i=0; i<(int)header.size(); ++i) {
        string c = toLower(header[i]);
        if (c.find("time") != string::npos || c.find("date") != string::npos) idx_time=i;
        else if (c.find("lat") != string::npos) idx_lat=i;
        else if (c.find("lon") != string::npos) idx_lon=i;
        else if (c.find("mag") != string::npos) idx_mag=i;
        else if (c.find("type") != string::npos) idx_type=i;
    }

    int n = lines.size() - 1;
    vector<Event> events(n);

    double t0 = omp_get_wtime();
#pragma omp parallel for schedule(dynamic)
    for (int i=1; i<=n; ++i) {
        vector<string> row = csvSplit(lines[i]);
        if ((int)row.size() <= max({idx_time, idx_lat, idx_lon, idx_mag, idx_type})) continue;
        Event e;
        e.raw_time = row[idx_time];
        e.date = extractDate(row[idx_time]);
        parseDouble(row[idx_lat], e.lat);
        parseDouble(row[idx_lon], e.lon);
        parseDouble(row[idx_mag], e.mag);
        e.type = toLower(row[idx_type]);
        e.valid = !(isnan(e.lat) || isnan(e.lon) || e.date.empty());
        events[i-1] = e;
    }

    double parse_ms = (omp_get_wtime() - t0) * 1000.0;

    unordered_map<string,long> type_count;
#pragma omp parallel
    {
        unordered_map<string,long> local_type;
#pragma omp for nowait
        for (int i=0;i<n;++i) {
            if (!events[i].valid) continue;
            local_type[events[i].type]++;
        }
#pragma omp critical
        for (auto &p:local_type) type_count[p.first]+=p.second;
    }

    double analyze_ms = (omp_get_wtime() - t0) * 1000.0;

    // SUMMARY
    cout << CYAN << "================== SUMMARY ==================\n" << RESET;
    cout << WHITE << "Total Events:  " << BOLD << YELLOW << n << RESET << "\n";
    cout << WHITE << "Event Type Distribution:\n";

    auto colorize = [&](const string &type) {
        if (type.find("earthquake") != string::npos) return RED;
        if (type.find("storm") != string::npos || type.find("cyclone") != string::npos) return BLUE;
        if (type.find("flood") != string::npos) return GREEN;
        if (type.find("fire") != string::npos || type.find("wild") != string::npos) return YELLOW;
        if (type.find("volcano") != string::npos) return MAGENTA;
        if (type.find("drought") != string::npos || type.find("landslide") != string::npos) return GRAY;
        if (type.find("ice") != string::npos) return WHITE;
        return WHITE;
    };

    vector<pair<string,long>> sortedTypes(type_count.begin(), type_count.end());
    sort(sortedTypes.begin(), sortedTypes.end(), [](auto&a,auto&b){return a.second>b.second;});
    for (auto &kv : sortedTypes)
        cout << "  " << colorize(kv.first) << setw(18) << left << kv.first
             << RESET << " : " << GREEN << kv.second << RESET << "\n";

    cout << "\n" << CYAN << "Top 10 Countries by Severity (from visualize_severity.py):\n" << RESET;
    ifstream top10("output/top10_countries.txt");
    if (top10.is_open()) {
        string l; while (getline(top10,l)) cout << "  " << WHITE << l << RESET << "\n";
        top10.close();
    } else {
        cout << GRAY << "  (Run 'python visualize_severity.py' first to generate data.)\n" << RESET;
    }

    cout << CYAN << "=============================================\n\n" << RESET;
    cout << GREEN << "[OK]" << RESET << " Parsing & Analysis Completed Successfully\n";
    cout << "     Parse time (ms): " << YELLOW << fixed << setprecision(2) << parse_ms << RESET << "\n";
    cout << "     Analyze time(ms): " << YELLOW << fixed << setprecision(2) << analyze_ms << RESET << "\n\n";

    // =================== WRITE KML ONLY ===================
    unordered_map<string,string> kmlColor = {
        {"earthquake", "ff0000ff"},
        {"storm",      "ff0066ff"},
        {"cyclone",    "ff0099ff"},
        {"flood",      "ff00ff00"},
        {"wildfire",   "ff00ffff"},
        {"volcano",    "ff8000ff"},
        {"drought",    "ffaaaaaa"},
        {"landslide",  "ff888888"},
        {"ice",        "ffffffff"},
        {"other",      "ff999999"}
    };

    unordered_map<string,double> typeBoost = {
        {"earthquake",1.0},{"volcano",1.8},{"wildfire",2.0},
        {"storm",2.3},{"cyclone",2.5},{"flood",2.5},
        {"drought",2.0},{"landslide",2.0},{"ice",2.0},{"other",2.0}
    };

    ofstream kml("output/hotspots.kml");
    kml << "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    kml << "<kml xmlns=\"http://www.opengis.net/kml/2.2\">\n<Document>\n";
    kml << "<name>Global Disaster Hotspots</name>\n";

    for (auto &e : events) {
        if (!e.valid) continue;
        string color="ffffffff";
        for (auto &kv:kmlColor) if (e.type.find(kv.first)!=string::npos) color=kv.second;
        double scale = isnan(e.mag) ? 1.0 : min(MAX_ICON_SCALE, max(MIN_ICON_SCALE, e.mag/3.0));
        double boost = typeBoost.count(e.type) ? typeBoost[e.type] : 1.8;
        scale *= boost;
        if (scale > MAX_ICON_SCALE) scale = MAX_ICON_SCALE;
        kml << "<Placemark>\n"
            << "<name>" << e.type << "</name>\n"
            << "<Style><IconStyle><color>" << color << "</color><scale>" << scale << "</scale>"
            << "<Icon><href>http://maps.google.com/mapfiles/kml/paddle/" << e.type[0] << ".png</href></Icon>"
            << "</IconStyle></Style>\n"
            << "<Point><coordinates>" << e.lon << "," << e.lat << ",0</coordinates></Point>\n"
            << "</Placemark>\n";
    }

    kml << "</Document></kml>\n";
    kml.close();

    // OUTPUT SECTION (Unchanged)
    cout << BOLD << WHITE << "[OUTPUT FILES]\n" << RESET;
    cout << "   `-- " << GREEN << "output/hotspots.kml\n\n" << RESET;

    cout << BOLD << WHITE << "[VISUALIZATION]\n" << RESET;
    cout << "   Open 'output/hotspots.kml' in Google Earth Pro\n";
    cout << "   or upload to: https://earth.google.com/web\n\n";

    cout << BOLD << WHITE << "[LEGEND - COLOR CODED HOTSPOTS]\n" << RESET;
    cout << RED    << "   [RED]     Earthquake\n";
    cout << BLUE   << "   [BLUE]    Storms/Cyclones\n";
    cout << GREEN  << "   [GREEN]   Floods\n";
    cout << YELLOW << "   [YELLOW]  Wildfires\n";
    cout << MAGENTA<< "   [PURPLE]  Volcano\n";
    cout << GRAY   << "   [GRAY]    Drought/Landslide\n";
    cout << WHITE  << "   [WHITE]   Sea/Ice/Other\n\n" << RESET;

    cout << CYAN << "--------------------------------------------------------\n";
    cout << "   Tip: Marker color = disaster type; marker size = severity (bigger = more severe).\n";
    cout << "--------------------------------------------------------\n" << RESET;

    return 0;
}
