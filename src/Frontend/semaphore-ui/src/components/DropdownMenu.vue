<template>
    <div class="dropdown">
      <button class="dropbtn" @click="toggleDropdown">
        <div class="bar"></div>
        <div class="bar"></div>
        <div class="bar"></div>
      </button>
      <div class="dropdown-content" v-if="isOpen">
        <div v-for="option in options" :key="option" @click="selectOption(option)">
          {{ option }}
        </div>
      </div>
    </div>
  </template>
  
  <script>
  export default {
    name: "DropdownMenu",
    props: {
      options: {
        type: Array,
        required: true,
      },
    },
    data() {
      return {
        isOpen: false,
        selectedOption: null,
      };
    },
    methods: {
      toggleDropdown() {
        this.isOpen = !this.isOpen;
      },
      selectOption(option) {
        this.selectedOption = option;
        this.isOpen = false;
        this.$emit("optionSelected", option); 
      },
    },
  };
  </script>
  
  <style scoped>
  .dropdown {
    position: relative;
    display: inline-block;
    float: right; /* Align to the right */
    margin: 10px; /* Add some margin for spacing */
  }
  
  .dropbtn {
    background-color: transparent;
    border: none;
    cursor: pointer;
    padding: 10px; 
  }
  
  .bar {
    display: block;
    width: 25px; 
    height: 3px; /* Height of the hamburger bars */
    background-color: #333; /* Color of the bars */
    margin: 4px auto; /* Spacing between the bars */
  }
  
  .dropdown-content {
    display: block;
    position: absolute;
    right: 0; /* Align dropdown to the right */
    background-color: white;
    box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    z-index: 1;
    min-width: 160px;
  }
  
  .dropdown-content div {
    color: black;
    padding: 12px 16px;
    text-decoration: none;
    cursor: pointer;
  }
  
  .dropdown-content div:hover {
    background-color: #f1f1f1;
  }
  </style>
  