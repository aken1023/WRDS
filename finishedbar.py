import matplotlib.pyplot as plt
import numpy as np

def create_progress_bar(percentage):
    # Set dark theme
    plt.style.use('dark_background')
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 1))
    
    # Create progress bar
    bar = ax.barh([0], [percentage], height=0.3, color='#00ff00')
    
    # Add background bar (gray)
    ax.barh([0], [100], height=0.3, color='#333333', zorder=1)
    ax.barh([0], [percentage], height=0.3, color='#00ff00', zorder=2)
    
    # Customize the chart
    ax.set_yticks([])
    ax.set_xticks([])
    
    # Add percentage text
    plt.text(50, 0, f'{percentage}%', 
             ha='center', va='center',
             fontsize=14, fontweight='bold',
             color='white')
    
    # Remove borders
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    
    # Set background color
    fig.patch.set_facecolor('black')
    ax.set_facecolor('black')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the figure
    plt.savefig(f'progress_{percentage}.png', 
                bbox_inches='tight',
                facecolor='black',
                edgecolor='none',
                pad_inches=0.1,
                dpi=100)
    plt.close()

# Create 10 different progress bars
for percentage in range(10, 101, 10):
    create_progress_bar(percentage)
