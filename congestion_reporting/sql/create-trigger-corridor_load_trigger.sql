﻿CREATE TRIGGER corridor_load_trigger AFTER INSERT ON here_analysis.corridor_load EXECUTE PROCEDURE populate_corridors();