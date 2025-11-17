import os
from datetime import datetime


def create_templates():
    """Create CSV template files"""
    # Create data directory
    os.makedirs('data', exist_ok=True)

    # Create behavioral.csv template
    behavioral_template = """timestamp,sleep_hours,breakfast_skipped,lunch_skipped,phone_usage,caffeine_count,steps,water_glasses,exercise
2025-11-17 08:00:00,7.5,N,N,0,1,500,2,N
2025-11-17 14:00:00,7.5,N,N,45,2,3500,4,N
2025-11-17 20:00:00,7.5,N,N,120,2,8000,6,Y"""

    with open('data/behavioral.csv', 'w') as f:
        f.write(behavioral_template)
    print("✓ Created data/behavioral.csv")

    # Create cognitive.csv template
    cognitive_template = """timestamp,brain_fog_score,reaction_time_ms,verbal_memory_words
2025-11-17 08:00:00,4,245,12
2025-11-17 14:00:00,6,280,10
2025-11-17 20:00:00,3,230,14"""

    with open('data/cognitive.csv', 'w') as f:
        f.write(cognitive_template)
    print("✓ Created data/cognitive.csv")

    print("""
📝 CSV Templates Created!

BEHAVIORAL DATA FIELDS:
- timestamp: YYYY-MM-DD HH:MM:SS (24-hour format)
- sleep_hours: decimal (0-24)
- breakfast_skipped: Y/N
- lunch_skipped: Y/N  
- phone_usage: integer (number of pickups)
- caffeine_count: integer (cups/drinks)
- steps: integer
- water_glasses: integer
- exercise: Y/N

COGNITIVE DATA FIELDS:
- timestamp: YYYY-MM-DD HH:MM:SS (must match behavioral!)
- brain_fog_score: 1-10 (1=clear, 10=very foggy)
- reaction_time_ms: milliseconds (from humanbenchmark.com)
- verbal_memory_words: integer (words remembered)

⚠️ IMPORTANT: Use the SAME timestamp for both files when recording data!
""")


if __name__ == "__main__":
    create_templates()

    # Verify files were created
    print("\nVerifying files...")
    if os.path.exists('data/behavioral.csv'):
        print("✓ behavioral.csv exists")
    if os.path.exists('data/cognitive.csv'):
        print("✓ cognitive.csv exists")